package database

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/carbonzipper/mlog"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/database/toc"
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/bloom"
	"github.com/kanatohodets/carbonsearch/index/full"
	"github.com/kanatohodets/carbonsearch/index/split"
	"github.com/kanatohodets/carbonsearch/tag"
	"github.com/kanatohodets/carbonsearch/util"
)

var logger mlog.Level

// Database abstracts over contained indexes
type Database struct {
	stats *util.Stats
	// not protected by a lock: it's read only once the database is created
	serviceToIndex map[string]index.Index

	queryLimit  int
	resultLimit int

	// not protected by a lock: it's read only once the database is created
	splitIndexes map[string]*split.Index

	fullIndexService string
	textIndexService string

	writeMut    sync.RWMutex
	writeBuffer *writeBuffer

	toc *toc.TableOfContents

	FullIndex *full.Index
	TextIndex *bloom.Index
}

// TrackPosition allows (kafka) consumers to report their `cur` position in processing to db
func (db *Database) TrackPosition(topic string, p int32, cur, new int64) {
	db.stats.Progress.Set(fmt.Sprintf("%s-%d-current", topic, p), util.ExpInt(cur))
	db.stats.Progress.Set(fmt.Sprintf("%s-%d-newest", topic, p), util.ExpInt(new))
}

/*
	Query takes a map like this:

		{
			"server": ["server-state:live", "server-hw:intel"],
			"lb": ["lb-pool:www"]
		}

	and the db.serviceToIndexMap, which looks like this:

		{
			"server": index.Index,
			"lb": index.Index
		}

		and creates a map of queries for each index, where a query is just a slice of tags, like this:

		{
			index.Index: ["server-state:live", "server-hw:intel", "lb-pool:www"],
			index.Index: ["custom-favorites:btyler"]
		}
*/

// Query takes a set of "service-tag:value" queries organized by "service".
// Eg. "service" => [ "service-tag:value", "service-tag:value" ]
// The appropriate search index is queried for each "service"'s set of queries.
// The ending set of results is intersected (AND) to produce the final results.
func (db *Database) Query(tagsByService map[string][]string) ([]string, error) {
	queriesByIndex := map[index.Index]*index.Query{}

	// translate from text queries to index.Query and from text services to index.Index
	for service, tags := range tagsByService {
		mappedIndex, ok := db.serviceToIndex[service]
		if !ok {
			logger.Logf("warning: there's no index for service %q. as a result, these tags will be ignored: %v", service, tags)
			logger.Logln("this means that no tags have been added to the database with this service; the producer has not started yet")
			continue
		}
		q, ok := queriesByIndex[mappedIndex]
		if ok {
			q.AddTags(tags)
		} else {
			queriesByIndex[mappedIndex] = index.NewQuery(tags)
		}
	}

	// query indexes, take intersection of metrics
	metricSets := make([][]index.Metric, 0, len(queriesByIndex))
	for targetIndex, query := range queriesByIndex {
		metrics, err := targetIndex.Query(query)
		if err != nil {
			return nil, fmt.Errorf("database: error while querying index %s: %s", targetIndex.Name(), err)
		}

		metricSets = append(metricSets, metrics)
	}

	metrics := index.IntersectMetrics(metricSets)

	stringMetrics, err := db.TextIndex.UnmapMetrics(metrics)
	// TODO(btyler): try to figure out how to annotate this error with better
	// information, since just seeing a random int64 will not be very handy
	if err != nil {
		return nil, err
	}

	textQueries, ok := tagsByService["text"]
	if ok {
		stringMetrics = db.TextIndex.Filter(textQueries, stringMetrics)
	}

	if len(stringMetrics) > db.resultLimit {
		return nil, fmt.Errorf("database: query selected %d metrics, which is over the limit of %d results in a single query", len(stringMetrics), db.resultLimit)
	}

	return stringMetrics, nil
}

// if consistency becomes an issue (symptoms: metrics without mappings, or
// queries with mysteriously lacking metrics), we can put a lock around AddMetrics
// that globally protects indexes from writes during materialization
// TODO: do these concurrently, then only do atomic swap when they're all done generating the thing
func (db *Database) MaterializeIndexes() {
	db.writeMut.RLock()
	defer db.writeMut.RUnlock()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go db.TextIndex.Materialize(wg, db.writeBuffer.MetricList())

	for name, index := range db.splitIndexes {
		buf, ok := db.writeBuffer.splits[name]
		if !ok {
			panic(fmt.Sprintf("there's an index without a matching write buffer. this is an error in the code that initializes the database/split indexes: it must call writeBuffer.AddSplitIndex(%q)", name))
		}
		wg.Add(1)
		go index.Materialize(wg, buf.joinToMetric, buf.tagToJoin)
	}

	wg.Add(1)
	go db.FullIndex.Materialize(wg, db.writeBuffer.full)
	wg.Wait()
}

// InsertMetrics TODO:...
//NOTE(nnuss) -- to me this is logically the right-hand or downstream side of the si
func (db *Database) InsertMetrics(msg *m.KeyMetric) error {
	if msg.Value == "" {
		return fmt.Errorf("database: metric batch has an empty join key value")
	}
	if len(msg.Metrics) == 0 {
		return fmt.Errorf("database: metric batch must have at least one metric")
	}

	// only happens in the write-side
	_, ok := db.splitIndexes[msg.Key]
	if !ok {
		return fmt.Errorf("database InsertMetrics: no split index for join key %q", msg.Key)
	}

	validMetrics := db.validateMetrics(msg.Metrics)

	db.writeMut.Lock()
	err := db.writeBuffer.BufferMetrics(msg.Key, msg.Value, validMetrics)
	db.writeMut.Unlock()
	if err != nil {
		//TODO(btyler): metric for metric add errors
		return fmt.Errorf("database: error buffering metric batch: %v", err)
	}

	db.stats.MetricMessages.Add(1)
	db.stats.MetricsIndexed.Add(int64(len(msg.Metrics)))
	return nil
}

// InsertTags TODO:...
//NOTE(nnuss) -- to me this is logically the left-hand or upstream side of the si
func (db *Database) InsertTags(msg *m.KeyTag) error {
	if msg.Value == "" {
		return fmt.Errorf("database: tag batch has an empty join key value")
	}
	if len(msg.Tags) == 0 {
		return fmt.Errorf("database: tag batch must have at least one tag")
	}

	si, ok := db.splitIndexes[msg.Key]
	if !ok {
		return fmt.Errorf("database InsertTags: no split index for join key %q", msg.Key)
	}

	// TODO(btyler) avoid parsing tags twice (once in validateServiceIndexPairs, again in BufferTags)
	err := db.validateServiceIndexPairs(msg.Tags, si, msg.Key)
	if err != nil {
		return fmt.Errorf("database: tag batch failed validation for index %q: %s", msg.Key, err)
	}

	validTags := db.validateTags(msg.Tags)

	db.writeMut.Lock()
	err = db.writeBuffer.BufferTags(msg.Key, msg.Value, validTags)
	db.writeMut.Unlock()
	if err != nil {
		//TODO(btyler): metric for tag add errors
		return fmt.Errorf("database: error buffering metric batch: %v", err)
	}

	db.stats.TagMessages.Add(1)
	db.stats.TagsIndexed.Add(int64(len(msg.Tags)))
	return nil
}

// InsertCustom makes a custom index association
func (db *Database) InsertCustom(msg *m.TagMetric) error {
	if len(msg.Metrics) == 0 {
		return fmt.Errorf("database: custom batch must have at least one metric")
	}
	if len(msg.Tags) == 0 {
		return fmt.Errorf("database: custom batch must have at least one tag")
	}

	err := db.validateServiceIndexPairs(msg.Tags, db.FullIndex, db.fullIndexService)
	if err != nil {
		return fmt.Errorf("database: custom batch failed validation: %s", err)
	}

	validMetrics := db.validateMetrics(msg.Metrics)
	validTags := db.validateTags(msg.Tags)

	db.writeMut.Lock()
	err = db.writeBuffer.BufferCustom(validTags, validMetrics)
	db.writeMut.Unlock()
	if err != nil {
		return fmt.Errorf("database: error buffering metric batch: %v", err)
	}

	db.stats.CustomMessages.Add(1)
	return nil
}

//TODO(btyler) it might be nice to change this to log on a per-broken metric
// basis, or if we have validations from other indexes, to have each of them
// generate a blacklist and report on why things failed validation.
func (db *Database) validateMetrics(metrics []string) []string {
	return db.TextIndex.ValidateMetrics(metrics)
}

func (db *Database) validateTags(tags []string) []string {
	validTags := make([]string, 0, len(tags))
	for _, rawTag := range tags {
		err := tag.Validate(rawTag)
		if err != nil {
			continue
		}
		validTags = append(validTags, rawTag)
	}

	return validTags
}

func (db *Database) validateServiceIndexPairs(tags []string, givenIndex index.Index, indexName string) error {
	for _, queryTag := range tags {
		service, _, _, err := tag.Parse(queryTag)
		if err != nil {
			return fmt.Errorf("database: tag parse error while validating service-tag pairs, skipping batch, %v", err)
		}

		mappedIndex, ok := db.serviceToIndex[service]
		if ok {
			if mappedIndex != givenIndex {
				return fmt.Errorf("database: service %q is mapped to the %v index, but something is writing to carbonsearch expecting it to be associated with the %q index.", service, mappedIndex.Name(), indexName)
			}
		} else {
			return fmt.Errorf("database: service %q has no mapped index, but something is writing to carbonsearch expecting it to be associated with the %q index. if this assocation is intended, add it to carbonsearch.yaml", service, indexName)
		}
	}

	return nil
}

// New initializes a new Database
func New(
	queryLimit, resultLimit int,
	fullIndexService, textIndexService string,
	splitIndexConfig map[string][]string,
	stats *util.Stats,
) *Database {
	serviceToIndex := make(map[string]index.Index)

	toc := toc.NewToC()

	fullIndex := full.NewIndex()
	if fullIndexService != "" {
		serviceToIndex[fullIndexService] = fullIndex
		toc.AddIndexServiceEntry("full", fullIndex.Name(), fullIndexService)
	}

	textIndex := bloom.NewIndex()
	if textIndexService != "" {
		serviceToIndex[textIndexService] = textIndex
	}

	writeBuffer := NewWriteBuffer(fullIndex.Name(), toc)
	splitIndexes := map[string]*split.Index{}
	for joinKey, services := range splitIndexConfig {
		index := split.NewIndex(joinKey)
		splitIndexes[joinKey] = index
		err := writeBuffer.AddSplitIndex(joinKey)
		if err != nil {
			panic(fmt.Sprintf("database: %v has already been loaded. This likely means the config file has %v listed multiple times", joinKey, joinKey))
		}

		for _, service := range services {
			_, ok := serviceToIndex[service]
			if ok {
				panic(fmt.Sprintf("database: service %v has already been attached to index %v. This likely means the config file has %q listed multiple times under %q in the 'split_indexes' section", service, joinKey, service, joinKey))

			}
			serviceToIndex[service] = index
			toc.AddIndexServiceEntry("split", joinKey, service)
		}
	}

	db := &Database{
		stats:          stats,
		serviceToIndex: serviceToIndex,

		queryLimit:  queryLimit,
		resultLimit: resultLimit,

		splitIndexes: splitIndexes,

		fullIndexService: fullIndexService,
		textIndexService: textIndexService,

		writeBuffer: writeBuffer,
		toc:         toc,

		FullIndex: fullIndex,
		TextIndex: textIndex,
	}
	go func() {
		for {
			time.Sleep(5 * time.Second)
			db.stats.Uptime.Add(5)
			for _, si := range db.splitIndexes {
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-generation", si.Name()), util.ExpInt(si.Generation()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-generation-time", si.Name()), util.ExpInt(si.GenerationTime()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-metrics", si.Name()), util.ExpInt(si.ReadableMetrics()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-tags", si.Name()), util.ExpInt(si.ReadableTags()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-join", si.Name()), util.ExpInt(si.ReadableJoins()))
			}

			db.stats.TextIndex.Set("generation", util.ExpInt(db.TextIndex.Generation()))
			db.stats.TextIndex.Set("generation-time", util.ExpInt(db.TextIndex.GenerationTime()))
			db.stats.TextIndex.Set("metrics-readable", util.ExpInt(db.TextIndex.ReadableMetrics()))

			db.stats.FullIndex.Set("generation", util.ExpInt(db.FullIndex.Generation()))
			db.stats.FullIndex.Set("generation-time", util.ExpInt(db.FullIndex.GenerationTime()))
			db.stats.FullIndex.Set("tags-readable", util.ExpInt(db.FullIndex.ReadableTags()))
		}
	}()
	return db
}

// TODO(btyler) convert tags to byte slices right away so hash functions don't need casting
func (db *Database) ParseQuery(query string) (map[string][]string, error) {
	/*
		parse something like this:
			'server-state:live.server-hw:intel.lb-pool:www'
		into a map of 'tags' like this:
			{
				"server": [ "server-state:live", "server-hw:intel"],
				"lb": ["lb-pool:www"]
			}

		where a 'tag' is a complete "prefix-key:value" item, such as "server-state:live".

		these will be used to search the "left" side of our indexes: tag -> [$join_key, $join_key...]
	*/

	//NOTE(btyler) v1 only supports (implicit) 'and': otherwise we need precedence rules and...yuck
	// additionally, you can get 'or' by adding more metrics to your query
	tags := strings.Split(query, ".")
	if len(tags) > db.queryLimit {
		return nil, fmt.Errorf(
			"database ParseQuery: max query size is %v, but this query has %v tags. try again with a smaller query",
			db.queryLimit,
			len(tags),
		)
	}

	tagsByService := make(map[string][]string)
	for _, queryTag := range tags {
		service, err := tag.ParseService(queryTag)
		if err != nil {
			return nil, err
		}

		db.stats.QueryTagsByService.Add(service, 1)

		_, ok := tagsByService[service]
		if !ok {
			tagsByService[service] = []string{}
		}

		tagsByService[service] = append(tagsByService[service], queryTag)
	}
	return tagsByService, nil
}

func (db *Database) TableOfContents() map[string]map[string]map[string]map[string]int {
	return db.toc.GetTable()
}

func (db *Database) MetricList() []string {
	db.writeMut.RLock()
	defer db.writeMut.RUnlock()
	return db.writeBuffer.MetricList()
}
