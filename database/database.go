package database

import (
	"fmt"
	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/full"
	"github.com/kanatohodets/carbonsearch/index/split"
	"github.com/kanatohodets/carbonsearch/index/text"
	"github.com/kanatohodets/carbonsearch/tag"
	"log"
	"sync"
)

type Database struct {
	serviceToIndex    map[string]index.Index
	serviceIndexMutex sync.RWMutex

	splitIndexes map[string]*split.Index
	splitMutex   sync.RWMutex

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
	queriesByIndex := map[index.Index][]string{}

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
			queriesByIndex[mappedIndex] = []string{}
		}
		queriesByIndex[mappedIndex] = append(queriesByIndex[mappedIndex], tags...)
	}
	db.serviceIndexMutex.RUnlock()

	metricCounts := make(map[string]int)
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

	var metricSet []string
	for metric, count := range metricCounts {
		if count == setMembershipThreshold {
			metricSet = append(metricSet, metric)
		}
	}

	// TODO(btyler): 're-match' should hit a text index of all metric names,
	// while 're-filter' should be the last-step filtering that it currently does.
	regexpTags, ok := tagsByService["re"]
	if ok {
		var err error
		metricSet, err = db.TextIndex.Filter(regexpTags, metricSet)
		if err != nil {
			return nil, fmt.Errorf("error while grepping: %s", err)
		}
	}

	return metricSet, nil
}

//TODO(btyler) -- do we want to auto-create indexes?
func (db *Database) InsertMetrics(msg *m.KeyMetric) error {
	index, err := db.GetOrCreateSplitIndex(msg.Key)
	if err != nil {
		return fmt.Errorf("could not/get create index for %s: %s", msg.Key, err)
	}

	err = index.AddMetrics(msg.Value, msg.Metrics)
	if err != nil {
		return fmt.Errorf("could not add metrics to metric side of index %q: %s", msg.Key, err)
	}

	return nil
}

func (db *Database) InsertTags(msg *m.KeyTag) error {
	index, err := db.GetOrCreateSplitIndex(msg.Key)
	if err != nil {
		return fmt.Errorf("could not get/create index for %q: %s", msg.Key, err)
	}

	validTags := db.validateServiceIndexPairs(msg.Tags, index)

	err = index.AddTags(msg.Value, validTags)
	if err != nil {
		return fmt.Errorf("could not add tags to tag side of index %q: %s", msg.Key, err)
	}

	return nil
}

func (db *Database) InsertCustom(msg *m.TagMetric) error {
	validTags := db.validateServiceIndexPairs(msg.Tags, db.FullIndex)

	err := db.FullIndex.Add(validTags, msg.Metrics)
	if err != nil {
		return fmt.Errorf("error while adding to custom index: %s", err)
	}

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
		}
	}

	return valid
}

func New() *Database {
	serviceToIndex := make(map[string]index.Index)

	textIndex := text.NewIndex()
	serviceToIndex["re"] = textIndex

	fullIndex := full.NewIndex()
	serviceToIndex["custom"] = fullIndex

	return &Database{
		serviceToIndex: serviceToIndex,

		splitIndexes: make(map[string]*split.Index),

		FullIndex: fullIndex,
		TextIndex: textIndex,
	}
}
