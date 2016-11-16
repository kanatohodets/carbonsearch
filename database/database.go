package database

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/carbonzipper/mlog"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
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

	metrics      map[index.Metric]string
	metricsMutex sync.RWMutex

	fullIndexService string

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
func (db *Database) MaterializeIndexes() error {
	// materialize the text index first, since it has the mapping for metric -> string
	err := db.TextIndex.Materialize()
	if err != nil {
		return fmt.Errorf("database: error while materializing text index %v: %v", db.TextIndex.Name(), err)
	}

	for _, index := range db.splitIndexes {
		err := index.Materialize()
		if err != nil {
			return fmt.Errorf("database: error while materializing split index %v: %v", index.Name(), err)
		}
	}
	return nil
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
	si, ok := db.splitIndexes[msg.Key]
	if !ok {
		return fmt.Errorf("database InsertMetrics: no split index for join key %q", msg.Key)
	}

	db.stats.MetricMessages.Add(1)

	// []string => []Metric
	metricHashes := index.HashMetrics(msg.Metrics)
	// add to text index, this should come before the split index adding so
	// that we're sure the index.Metric->string mapping contains this metric
	err := db.TextIndex.AddMetrics(msg.Metrics, metricHashes)
	if err != nil {
		return fmt.Errorf("database: could not add metrics to text index: %s", err)
	}

	// add metricHashes to right-side[msg.Value]
	err = si.AddMetrics(msg.Value, metricHashes)
	if err != nil {
		return fmt.Errorf("database: could not add metrics to metric side of index %q: %s", msg.Key, err)
	}

	db.stats.MetricsIndexed.Add(int64(len(metricHashes)))
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

	db.stats.TagMessages.Add(1)

	tags := db.validateServiceIndexPairs(msg.Tags, si, msg.Key)

	err := si.AddTags(msg.Value, tags)
	if err != nil {
		return fmt.Errorf("database: could not add tags to tag side of index %q: %s", msg.Key, err)
	}

	db.stats.TagsIndexed.Add(int64(len(tags)))
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

	tags := index.HashTags(db.validateServiceIndexPairs(msg.Tags, db.FullIndex, db.fullIndexService))

	db.stats.CustomMessages.Add(1)

	metricHashes := index.HashMetrics(msg.Metrics)

	// add to text index, this should come before the full index adding so
	// that we're sure the index.Metric->string mapping contains this metric
	err := db.TextIndex.AddMetrics(msg.Metrics, metricHashes)
	if err != nil {
		return fmt.Errorf("database: could not add metrics to text index: %s", err)
	}

	err = db.FullIndex.Add(tags, metricHashes)
	if err != nil {
		return fmt.Errorf("database: error while adding to custom index: %s", err)
	}

	db.stats.FullIndexTags.Set(int64(db.FullIndex.TagSize()))
	db.stats.FullIndexMetrics.Set(int64(db.FullIndex.MetricSize()))

	return nil
}

// NOTE(btyler): we're being permissive here and only skipping adding tags with
// a bad service for this index. this contrasts with discarding the entire update.
func (db *Database) validateServiceIndexPairs(tags []string, givenIndex index.Index, indexName string) []string {
	valid := []string{}
	for _, queryTag := range tags {
		service, _, _, err := tag.Parse(queryTag)
		if err != nil {
			logger.Logln("database: tag parse error while validating service-tag pairs, skipping ", err)
			continue
		}

		mappedIndex, ok := db.serviceToIndex[service]
		if ok {
			if mappedIndex == givenIndex {
				valid = append(valid, queryTag)
			}
		} else {
			logger.Logf("database: service %q has no mapped index, but something is writing to carbonsearch expecting it to be associated with the %q index. if this assocation is intended, add it to config.yaml", service, indexName)
			continue
		}
	}

	return valid
}

// New initializes a new Database
func New(
	queryLimit, resultLimit int,
	fullIndexService, textIndexService string,
	splitIndexConfig map[string]string,
	stats *util.Stats,
) *Database {
	serviceToIndex := make(map[string]index.Index)

	fullIndex := full.NewIndex()
	if fullIndexService != "" {
		serviceToIndex[fullIndexService] = fullIndex
	}

	textIndex := bloom.NewIndex()
	if textIndexService != "" {
		serviceToIndex[textIndexService] = textIndex
	}

	splitIndexes := map[string]*split.Index{}
	for joinKey, service := range splitIndexConfig {
		index := split.NewIndex(joinKey)
		splitIndexes[joinKey] = index
		serviceToIndex[service] = index
	}

	db := &Database{
		stats:          stats,
		serviceToIndex: serviceToIndex,

		queryLimit:  queryLimit,
		resultLimit: resultLimit,

		splitIndexes: splitIndexes,

		metrics: make(map[index.Metric]string),

		fullIndexService: fullIndexService,

		FullIndex: fullIndex,
		TextIndex: textIndex,
	}
	go func() {
		for {
			time.Sleep(5 * time.Second)
			db.stats.Uptime.Add(5)
			for _, si := range db.splitIndexes {
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-generation", si.Name()), util.ExpInt(si.Generation()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-metrics-readable", si.Name()), util.ExpInt(si.ReadableMetrics()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-metrics-written", si.Name()), util.ExpInt(si.WrittenMetrics()))

				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-tags-readable", si.Name()), util.ExpInt(si.ReadableTags()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-tags-written", si.Name()), util.ExpInt(si.WrittenTags()))

				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-join-readable", si.Name()), util.ExpInt(si.ReadableJoins()))
				db.stats.SplitIndexes.Set(fmt.Sprintf("%s-join-written", si.Name()), util.ExpInt(si.WrittenJoins()))
			}

			db.stats.TextIndex.Set("generation", util.ExpInt(db.TextIndex.Generation()))
			db.stats.TextIndex.Set("metrics-readable", util.ExpInt(db.TextIndex.ReadableMetrics()))
			db.stats.TextIndex.Set("metrics-written", util.ExpInt(db.TextIndex.WrittenMetrics()))
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
