package database

import (
	"fmt"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/full"
	"github.com/kanatohodets/carbonsearch/index/split"
	"github.com/kanatohodets/carbonsearch/index/text"
	"github.com/kanatohodets/carbonsearch/tag"
	"github.com/kanatohodets/carbonsearch/util"
	"log"
	"sync"
)

type Database struct {
	stats             *util.Stats
	serviceToIndex    map[string]index.Index
	serviceIndexMutex sync.RWMutex

	splitIndexes map[string]*split.Index
	splitMutex   sync.RWMutex

	metrics      map[index.Metric]string
	metricsMutex sync.RWMutex

	FullIndex *full.Index
	TextIndex *text.Index
}

func (db *Database) GetOrCreateSplitIndex(join string) (*split.Index, error) {
	index := db.GetSplitIndex(join)
	if index == nil {
		var err error
		index, err = db.CreateSplitIndex(join)
		if err != nil {
			return nil, err
		}
	}
	return index, nil

}

func (db *Database) CreateSplitIndex(join string) (*split.Index, error) {
	db.splitMutex.Lock()
	defer db.splitMutex.Unlock()

	_, ok := db.splitIndexes[join]
	if ok {
		return nil, fmt.Errorf("index for key %s already exists", join)
	}

	index := split.NewIndex(join)
	db.splitIndexes[join] = index

	return index, nil
}

func (db *Database) GetSplitIndex(join string) *split.Index {
	db.splitMutex.RLock()
	defer db.splitMutex.RUnlock()

	index, ok := db.splitIndexes[join]
	if !ok {
		return nil
	}

	return index
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

func (db *Database) Query(tagsByService map[string][]string) ([]string, error) {
	queriesByIndex := map[index.Index][]index.Tag{}

	db.serviceIndexMutex.RLock()
	for service, tags := range tagsByService {
		mappedIndex, ok := db.serviceToIndex[service]
		if !ok {
			log.Printf("warning: there's no index for service %q. as a result, these tags will be ignored: %v", service, tags)
			log.Println("this means that no tags have been added to the database with this service; the producer has not started yet")
			continue
		}
		_, ok = queriesByIndex[mappedIndex]
		if !ok {
			queriesByIndex[mappedIndex] = []index.Tag{}
		}
		hashedTags := index.HashTags(tags)
		queriesByIndex[mappedIndex] = append(queriesByIndex[mappedIndex], hashedTags...)
	}
	db.serviceIndexMutex.RUnlock()

	metricCounts := make(map[index.Metric]int)
	setMembershipThreshold := 0

	// query indexes, take intersection of metrics
	for targetIndex, query := range queriesByIndex {
		// TODO(btyler) -- text index isn't a proper index yet, but just
		// a filter on the set that the other things spit out.
		if targetIndex.Name() == "text index" {
			continue
		}

		setMembershipThreshold++

		metrics, err := targetIndex.Query(query)
		if err != nil {
			return nil, fmt.Errorf("error while querying index %s: %s", targetIndex.Name(), err)
		}

		for _, metric := range metrics {
			metricCounts[metric]++
		}
	}

	var metricSet []index.Metric
	for metric, count := range metricCounts {
		if count == setMembershipThreshold {
			metricSet = append(metricSet, metric)
		}
	}

	stringMetrics, err := db.unmapMetrics(metricSet)
	// TODO(btyler): try to figure out how to annotate this error with better
	// information, since just seeing a random int64 will not be very handy
	if err != nil {
		return nil, err
	}

	// TODO(btyler): 're-match' should hit a text index of all metric names,
	// while 're-filter' should be the last-step filtering that it currently does.
	regexpTags, ok := tagsByService["re"]
	if ok {
		var err error
		stringMetrics, err = db.TextIndex.Filter(regexpTags, stringMetrics)
		if err != nil {
			return nil, fmt.Errorf("error while grepping: %s", err)
		}
	}

	return stringMetrics, nil
}

//TODO(btyler) -- do we want to auto-create indexes?
func (db *Database) InsertMetrics(msg *m.KeyMetric) error {
	si, err := db.GetOrCreateSplitIndex(msg.Key)
	if err != nil {
		return fmt.Errorf("could not/get create index for %s: %s", msg.Key, err)
	}

	db.stats.MetricMessages.Add(1)

	metrics := db.mapMetrics(msg.Metrics)
	err = si.AddMetrics(msg.Value, metrics)
	if err != nil {
		return fmt.Errorf("could not add metrics to metric side of index %q: %s", msg.Key, err)
	}

	db.stats.MetricsIndexed.Add(int64(len(metrics)))
	db.stats.SplitIndexes.Set(fmt.Sprintf("%s-metrics", si.Name()), util.ExpInt(si.MetricSize()))

	return nil
}

func (db *Database) InsertTags(msg *m.KeyTag) error {
	si, err := db.GetOrCreateSplitIndex(msg.Key)
	if err != nil {
		return fmt.Errorf("could not get/create index for %q: %s", msg.Key, err)
	}

	db.stats.TagMessages.Add(1)

	tags := index.HashTags(db.validateServiceIndexPairs(msg.Tags, si))

	err = si.AddTags(msg.Value, tags)
	if err != nil {
		return fmt.Errorf("could not add tags to tag side of index %q: %s", msg.Key, err)
	}

	db.stats.TagsIndexed.Add(int64(len(tags)))
	db.stats.SplitIndexes.Set(fmt.Sprintf("%s-tags", si.Name()), util.ExpInt(si.TagSize()))

	return nil
}

func (db *Database) InsertCustom(msg *m.TagMetric) error {
	tags := index.HashTags(db.validateServiceIndexPairs(msg.Tags, db.FullIndex))

	db.stats.CustomMessages.Add(1)

	metrics := db.mapMetrics(msg.Metrics)
	err := db.FullIndex.Add(tags, metrics)
	if err != nil {
		return fmt.Errorf("error while adding to custom index: %s", err)
	}

	db.stats.FullIndexTags.Set(int64(db.FullIndex.TagSize()))
	db.stats.FullIndexMetrics.Set(int64(db.FullIndex.MetricSize()))

	return nil
}

// ensure that tags are only added to one index -- the one that owns the tag's
// service, where 'server-state:live' has a service 'server'.
// NOTE(btyler): we're being permissive here and only skipping adding tags with
// a bad service for this index. this contrasts with discarding the entire update.
func (db *Database) validateServiceIndexPairs(tags []string, givenIndex index.Index) []string {
	valid := []string{}
	for _, queryTag := range tags {
		service, _, err := tag.Parse(queryTag)
		if err != nil {
			log.Println("tag parse error while validating service-tag pairs, skipping ", err)
			continue
		}

		db.serviceIndexMutex.RLock()
		mappedIndex, ok := db.serviceToIndex[service]
		if ok {
			if mappedIndex == givenIndex {
				valid = append(valid, queryTag)
			}
			db.serviceIndexMutex.RUnlock()
		} else {
			db.serviceIndexMutex.RUnlock()
			// first seen -> correct till end of time. this assumption may not scale.
			db.serviceIndexMutex.Lock()
			db.serviceToIndex[service] = givenIndex
			db.serviceIndexMutex.Unlock()

			valid = append(valid, queryTag)

			db.stats.ServicesByIndex.Set(service, util.ExpString(givenIndex.Name()))
		}
	}

	return valid
}

func (db *Database) mapMetrics(metrics []string) []index.Metric {
	db.metricsMutex.Lock()
	defer db.metricsMutex.Unlock()

	hashed := make([]index.Metric, len(metrics))

	for i, metric := range metrics {
		hash := index.HashMetric(metric)
		db.metrics[hash] = metric
		hashed[i] = hash
	}

	return hashed
}

func (db *Database) unmapMetrics(metrics []index.Metric) ([]string, error) {
	db.metricsMutex.RLock()
	defer db.metricsMutex.RUnlock()

	stringMetrics := make([]string, len(metrics))

	for i, metric := range metrics {
		str, ok := db.metrics[metric]
		if !ok {
			return nil, fmt.Errorf("agh the hashed metric %q has no mapping back to a string! this is awful!", metric)
		}
		stringMetrics[i] = str
	}

	return stringMetrics, nil
}

func New(stats *util.Stats) *Database {
	serviceToIndex := make(map[string]index.Index)

	textIndex := text.NewIndex()
	serviceToIndex["text"] = textIndex

	fullIndex := full.NewIndex()
	serviceToIndex["custom"] = fullIndex

	return &Database{
		stats:          stats,
		serviceToIndex: serviceToIndex,

		splitIndexes: make(map[string]*split.Index),

		metrics: make(map[index.Metric]string),

		FullIndex: fullIndex,
		TextIndex: textIndex,
	}
}
