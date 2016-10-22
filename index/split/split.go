package split

/*

this package implements a simple split index. "split" because we store two mini-indexes
and join them at search time using the join key for that index.

join keys can be anything you use to associate some metrics with some tags. Our first
common case was hostname: associating metrics sent by a host with
properties of that host (liveness, dc, rack ID, etc.)

the two pieces of the index look like this (using hostname as an example):

"left" side of the join (tags to $join_key, in this case hostnames)
left: {
	server-state:live: [hostname-1234, hostname-1235, ...]
}

"right" side of the join ($join_key to metrics)
right: {
	hostname-1234: ["server.hostname-1234.cpu.i7z", ...]
}

the query process goes like this:

1) user searches for metrics that match a set of tags ("server-state:live", "server-dc:lhr")
2) we reach into the "left" index to fetch all of the join keys (hostnames) associated with all of these tags
3) take that intersection of join keys to find all the metrics associated with them
4) success! return that set of metrics

*/

import (
	"fmt"
	"sync"

	"github.com/kanatohodets/carbonsearch/index"
)

type Index struct {
	joinKey string

	tagToJoin map[index.Hash][]index.Hash
	tagMutex  sync.RWMutex
	tagCount  int

	joinToMetric map[index.Hash][]index.Hash
	metricMutex  sync.RWMutex
	metricCount  int
}

func NewIndex(joinKey string) *Index {
	return &Index{
		joinKey: joinKey,

		tagToJoin: make(map[index.Hash][]index.Hash),

		joinToMetric: make(map[index.Hash][]index.Hash),
	}
}

func (si *Index) AddMetrics(rawJoin string, metrics []index.Hash) error {
	if len(metrics) == 0 {
		return fmt.Errorf("split index: cannot add 0 metrics to join %q", rawJoin)
	}

	join := index.HashString(rawJoin)

	si.metricMutex.Lock()
	defer si.metricMutex.Unlock()

	metricList, ok := si.joinToMetric[join]
	if !ok {
		metricList = []index.Hash{}
		si.joinToMetric[join] = metricList
	}

	existingMember := make(map[index.Hash]bool)

	for _, metric := range metricList {
		existingMember[metric] = true
	}

	for _, metric := range metrics {
		_, ok := existingMember[metric]
		if !ok {
			si.metricCount++
			metricList = append(metricList, metric)
		}
	}

	index.SortHashes(metricList)
	si.joinToMetric[join] = metricList

	return nil
}

func (si *Index) AddTags(rawJoin string, tags []index.Hash) error {
	if len(tags) == 0 {
		return fmt.Errorf("split index: cannot add 0 tags to join %q", rawJoin)
	}

	join := index.HashString(rawJoin)

	si.tagMutex.Lock()
	defer si.tagMutex.Unlock()

	for _, tag := range tags {
		joinList, ok := si.tagToJoin[tag]
		if !ok {
			si.tagCount++
			joinList = []index.Hash{}
			si.tagToJoin[tag] = joinList
		}

		found := false
		for _, existingJoin := range joinList {
			if existingJoin == join {
				found = true
			}
		}

		if !found {
			joinList = append(joinList, join)
		}
		index.SortHashes(joinList)
		si.tagToJoin[tag] = joinList
	}

	return nil
}

func (si *Index) Query(q *index.Query) ([]index.Hash, error) {
	// get a slice of all the join keys (for example, hostnames) associated with these tags
	joinLists := [][]index.Hash{}
	si.tagMutex.RLock()
	for _, tag := range q.Tags {
		list, ok := si.tagToJoin[tag]
		if ok {
			joinLists = append(joinLists, list)
		}
	}
	si.tagMutex.RUnlock()

	// intersect join keys
	joinSet := index.IntersectHashes(joinLists)

	si.metricMutex.RLock()
	// deduplicated union all of the metrics associated with those join keys
	metricSets := [][]index.Hash{}
	for _, join := range joinSet {
		list, ok := si.joinToMetric[join]
		if ok {
			metricSets = append(metricSets, list)
		}
	}
	si.metricMutex.RUnlock()

	// map keys -> slice. except these need to be sorted, blorg!
	metrics := index.UnionHashes(metricSets)
	return metrics, nil
}

func (si *Index) Name() string {
	return si.joinKey
}

func (si *Index) TagSize() int {
	// or convert the sizes to atomics
	si.tagMutex.RLock()
	defer si.tagMutex.RUnlock()
	return si.tagCount
}

func (si *Index) MetricSize() int {
	// or convert the sizes to atomics
	si.metricMutex.RLock()
	defer si.metricMutex.RUnlock()
	return si.metricCount
}
