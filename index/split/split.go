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
	"container/heap"
	"fmt"
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/util"
	"sort"
	"sync"
)

type Join uint64
type JoinSlice []Join

func (a JoinSlice) Len() int           { return len(a) }
func (a JoinSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a JoinSlice) Less(i, j int) bool { return a[i] < a[j] }

type Index struct {
	joinKey string

	tagToJoin map[index.Tag][]Join
	tagMutex  sync.RWMutex
	tagCount  int

	joinToMetric map[Join][]index.Metric
	metricMutex  sync.RWMutex
	metricCount  int
}

func NewIndex(joinKey string) *Index {
	return &Index{
		joinKey: joinKey,

		tagToJoin: make(map[index.Tag][]Join),

		joinToMetric: make(map[Join][]index.Metric),
	}
}

func (i *Index) AddMetrics(rawJoin string, metrics []index.Metric) error {
	if len(metrics) == 0 {
		return fmt.Errorf("split index: cannot add 0 metrics to join %q", rawJoin)
	}

	join := HashJoin(rawJoin)

	i.metricMutex.Lock()
	defer i.metricMutex.Unlock()

	metricList, ok := i.joinToMetric[join]
	if !ok {
		metricList = []index.Metric{}
		i.joinToMetric[join] = metricList
	}

	existingMember := make(map[index.Metric]bool)

	for _, metric := range metricList {
		existingMember[metric] = true
	}

	for _, metric := range metrics {
		_, ok := existingMember[metric]
		if !ok {
			i.metricCount++
			metricList = append(metricList, metric)
		}
	}

	index.SortMetrics(metricList)
	i.joinToMetric[join] = metricList

	return nil
}

func (i *Index) AddTags(rawJoin string, tags []index.Tag) error {
	if len(tags) == 0 {
		return fmt.Errorf("split index: cannot add 0 tags to join %q", rawJoin)
	}

	join := HashJoin(rawJoin)

	i.tagMutex.Lock()
	defer i.tagMutex.Unlock()

	for _, tag := range tags {
		joinList, ok := i.tagToJoin[tag]
		if !ok {
			i.tagCount++
			joinList = []Join{}
			i.tagToJoin[tag] = joinList
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
		SortJoins(joinList)
		i.tagToJoin[tag] = joinList
	}

	return nil
}

func (i *Index) Query(query []index.Tag) ([]index.Metric, error) {
	// get a slice of all the join keys (for example, hostnames) associated with these tags
	joinLists := [][]Join{}
	i.tagMutex.RLock()
	for _, tag := range query {
		list, ok := i.tagToJoin[tag]
		if ok {
			joinLists = append(joinLists, list)
		}
	}
	i.tagMutex.RUnlock()

	// intersect join keys
	joinSet := IntersectJoins(joinLists)

	i.metricMutex.RLock()
	// deduplicated union all of the metrics associated with those join keys
	metricSets := [][]index.Metric{}
	for _, join := range joinSet {
		list, ok := i.joinToMetric[join]
		if ok {
			metricSets = append(metricSets, list)
		}
	}
	i.metricMutex.RUnlock()

	// map keys -> slice. except these need to be sorted, blorg!
	metrics := index.UnionMetrics(metricSets)
	return metrics, nil
}

func (i *Index) Name() string {
	return i.joinKey
}

func (i *Index) TagSize() int {
	// or convert the sizes to atomics
	i.tagMutex.RLock()
	defer i.tagMutex.RUnlock()
	return i.tagCount
}

func (i *Index) MetricSize() int {
	// or convert the sizes to atomics
	i.metricMutex.RLock()
	defer i.metricMutex.RUnlock()
	return i.metricCount
}

func HashJoin(join string) Join {
	return Join(util.HashStr64(join))
}

func HashJoins(joins []string) []Join {
	result := make([]Join, len(joins))
	for i, join := range joins {
		result[i] = HashJoin(join)
	}
	return result
}

func SortJoins(joins []Join) {
	sort.Sort(JoinSlice(joins))
}

type JoinSetsHeap [][]Join

func (h JoinSetsHeap) Len() int           { return len(h) }
func (h JoinSetsHeap) Less(i, j int) bool { return h[i][0] < h[j][0] }
func (h JoinSetsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *JoinSetsHeap) Push(x interface{}) {
	t := x.([]Join)
	*h = append(*h, t)
}

func (h *JoinSetsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func IntersectJoins(joinSets [][]Join) []Join {
	if len(joinSets) == 0 {
		return []Join{}
	}

	h := &JoinSetsHeap{}
	heap.Init(h)
	for _, list := range joinSets {
		// any empty set --> empty intersection
		if len(list) == 0 {
			return []Join{}
		}
		heap.Push(h, list)
	}
	set := []Join{}
	for {
		cur := (*h)[0]
		smallestJoin := cur[0]
		present := 0
		fixups := make([]bool, h.Len())
		for i, candidate := range *h {
			if candidate[0] <= smallestJoin {
				fixups[i] = true
			}
			if candidate[0] == smallestJoin {
				present++
			}
		}

		// found something in every subset
		if present == len(joinSets) {
			if len(set) == 0 || set[len(set)-1] != smallestJoin {
				set = append(set, smallestJoin)
			}
		}

		for i, fix := range fixups {
			if fix {
				list := (*h)[i]
				if len(list) == 1 {
					return set
				}
				(*h)[i] = list[1:]
				heap.Fix(h, i)
			}
		}
	}
}
