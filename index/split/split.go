package split

/*

this package implements a simple split index. "split" because we store two mini-indexes
{[left], [right]} and join them at search time using the join key for that index.

join keys can be anything you use to associate some metrics with some tags. Our first
common case was hostname: associating metrics sent by a host with
properties of that host (liveness, dc, rack ID, etc.)

the two pieces of the index look like this (using hostname as an example):

"left" side of the join (tags to $join_key, in this case hostnames)
[left]: {
	server-state:live: [hostname-1234, hostname-1235, ...]
}

"right" side of the join ($join_key to metrics)
[right]: {
	hostname-1234: ["server.hostname-1234.cpu.i7z", ...]
}

the query process goes like this:

1) user searches for metrics that match a set of tags ("server-state:live", "server-dc:lhr")
2) we reach into the "left" index to fetch all of the join keys (hostnames) associated with all of these tags
3) take that intersection of join keys to find all the metrics associated with them
4) success! return that set of metrics

search for tag in
	?
	v
 [left]
	=
	v
value is lists of join keys
	|
	v
< intersect those lists of join keys >
	=
	v
search for join keys in
	?
	v
 [right]
	=
	v
value is lists of metrics
	|
	v
< intersect those lists of metrics >
	=
	v
return final intersected set


!<-LEFT-LEFT-LEFT-LEFT->!
!   Tag  =>    JoinKey  !
-------------------------------------
            |  JoinKey    => Metric |
            |<-RIGHT-RIGHT-RIGHT--->|
*/

import (
	"container/heap"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/tag"
	"github.com/kanatohodets/carbonsearch/util"
)

type Join uint64
type JoinSlice []Join

func (a JoinSlice) Len() int           { return len(a) }
func (a JoinSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a JoinSlice) Less(i, j int) bool { return a[i] < a[j] }

type Index struct {
	joinKey    string
	generation uint64

	tagToJoin atomic.Value //map[index.Tag][]Join
	tagCount  uint32

	joinToMetric atomic.Value //map[Join][]index.Metric
	joinCount    uint32

	metricCount uint32
	// Mutable

	// we want "service-key" => Join => "service-key:val"
	// to be able to support "set" at the service-key level
	// "service-key:blue" un-sets "service-key:orange"
	// The common case is there is 1 index.Tag
	tagToJoinMutable map[tag.ServiceKey]map[Join][]index.Tag
	tagMutex         sync.RWMutex

	joinToMetricMutable map[Join][]index.Metric
	metricMutex         sync.RWMutex
}

func NewIndex(joinKey string) *Index {
	n := Index{
		joinKey: joinKey,

		tagToJoinMutable: make(map[tag.ServiceKey]map[Join][]index.Tag),

		joinToMetricMutable: make(map[Join][]index.Metric),
	}
	n.tagToJoin.Store(make(map[index.Tag][]Join))
	n.joinToMetric.Store(make(map[Join][]index.Metric))

	go func(in *Index) {
		for {
			time.Sleep(time.Minute)
			in.newGeneration()
		}
	}(&n)

	log.Println("shiny new g", n.generation)

	return &n
}

// AddMetrics adds documents to the rawJoin entry in the metric/right-side index in si.
// TODO(nnuss): len(metrics) == 0 is the only error condition and can probably be removed as adding zero things is actually easy.
// Also this and AddTags should be merged.
func (si *Index) AddMetrics(rawJoin string, metrics []index.Metric) error {
	if len(metrics) == 0 {
		return fmt.Errorf("split index: cannot add 0 metrics to join %q", rawJoin)
	}

	join := HashJoin(rawJoin)

	si.metricMutex.Lock()
	defer si.metricMutex.Unlock()

	metricList, ok := si.joinToMetricMutable[join]
	if !ok {
		metricList = []index.Metric{}
		si.joinToMetricMutable[join] = metricList
	}

	existingMember := make(map[index.Metric]bool)

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

	index.SortMetrics(metricList)
	si.joinToMetricMutable[join] = metricList

	return nil
}

// AddTags adds rawJoin to the value of { every one of tags[] } entries in the tag/left-side index of si
// argh this needs to have the actual Tag
func (si *Index) AddTags(rawJoin string, tags []string) error {
	if len(tags) == 0 {
		return fmt.Errorf("split index: cannot add 0 tags to join %q", rawJoin)
	}

	join := HashJoin(rawJoin)

	si.tagMutex.Lock()
	defer si.tagMutex.Unlock()

	for _, t := range tags {
		s, k, _, err := tag.Parse(t)
		if err != nil {
			continue
		}
		sk := HashServiceKey(s + "-" + k)
		joinList, ok := si.tagToJoinMutable[sk]
		if !ok {
			joinList = make(map[Join][]index.Tag)
			si.tagToJoinMutable[sk] = joinList
		}
		joinList[join] = index.HashTags(tags)
	}

	return nil
}

// newGeneration copies the mutable indexes to new read-only ones and swaps out the existing ones.
func (si *Index) newGeneration() error {
	start := time.Now()
	tagToJoin := make(map[index.Tag][]Join)
	si.tagMutex.RLock()
	defer si.tagMutex.RUnlock()
	for _, m := range si.tagToJoinMutable {
		for join, tags := range m {
			for _, tag := range tags {
				tagToJoin[tag] = append(tagToJoin[tag], join)
			}
		}
	}

	joinToMetric := make(map[Join][]index.Metric)
	si.metricMutex.RLock()
	defer si.metricMutex.RUnlock()
	var metricCount uint32
	for k, v := range si.joinToMetricMutable {
		joinToMetric[k] = v
		metricCount += uint32(len(v))
	}

	si.tagToJoin.Store(tagToJoin)
	si.joinToMetric.Store(joinToMetric)
	g := atomic.AddUint64(&si.generation, 1) // timestamp based maybe
	_ = atomic.SwapUint32(&si.tagCount, uint32(len(tagToJoin)))
	_ = atomic.SwapUint32(&si.joinCount, uint32(len(joinToMetric)))
	_ = atomic.SwapUint32(&si.metricCount, metricCount)

	elapsed := time.Since(start)
	log.Println("New generation", g, "took", elapsed)

	return nil
}

func (si *Index) Query(q *index.Query) ([]index.Metric, error) {
	// get a slice of all the join keys (for example, hostnames) associated with these tags
	tagToJoin := si.TagIndex()
	joinLists := [][]Join{}
	for _, tag := range q.Hashed {
		list, ok := tagToJoin[tag]
		if ok {
			joinLists = append(joinLists, list)
		}
	}

	// intersect join keys
	joinSet := IntersectJoins(joinLists)

	// deduplicated union all of the metrics associated with those join keys
	joinToMetric := si.MetricIndex()
	metricSets := [][]index.Metric{}
	for _, join := range joinSet {
		list, ok := joinToMetric[join]
		if ok {
			metricSets = append(metricSets, list)
		}
	}

	// map keys -> slice. except these need to be sorted, blorg!
	metrics := index.UnionMetrics(metricSets)
	return metrics, nil
}

func (si *Index) TagIndex() map[index.Tag][]Join {
	return si.tagToJoin.Load().(map[index.Tag][]Join)
}
func (si *Index) MetricIndex() map[Join][]index.Metric {
	return si.joinToMetric.Load().(map[Join][]index.Metric)
}

func (si *Index) Name() string {
	return si.joinKey
}

func (si *Index) TagSize() int {
	return int(si.tagCount)
}

func (si *Index) MetricSize() int {
	return int(si.metricCount)
}

func HashJoin(join string) Join {
	return Join(util.HashStr64(join))
}

func HashServiceKey(svcKey string) tag.ServiceKey {
	return tag.ServiceKey(util.HashStr64(svcKey))
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

	for _, list := range joinSets {
		// any empty set --> empty intersection
		if len(list) == 0 {
			return []Join{}
		}
	}

	h := JoinSetsHeap(joinSets)
	heap.Init(&h)
	set := []Join{}
	for {
		cur := h[0]
		smallestJoin := cur[0]
		present := 0
		for _, candidate := range h {
			if candidate[0] == smallestJoin {
				present++
			} else {
				// any further matches will be purged by the fixup loop
				break
			}
		}

		// found something in every subset
		if present == len(joinSets) {
			if len(set) == 0 || set[len(set)-1] != smallestJoin {
				set = append(set, smallestJoin)
			}
		}

		for h[0][0] == smallestJoin {
			list := h[0]
			if len(list) == 1 {
				return set
			}

			h[0] = list[1:]
			heap.Fix(&h, 0)
		}
	}
}
