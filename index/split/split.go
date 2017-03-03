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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/carbonzipper/mlog"
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/tag"
	"github.com/kanatohodets/carbonsearch/util"
)

var logger mlog.Level

type Join uint64
type JoinSlice []Join

func (a JoinSlice) Len() int           { return len(a) }
func (a JoinSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a JoinSlice) Less(i, j int) bool { return a[i] < a[j] }

type Index struct {
	joinKey        string
	generation     uint64
	generationTime int64 // time.Duration

	tagToJoin    atomic.Value //map[index.Tag][]Join
	readableTags uint32

	joinToMetric  atomic.Value //map[Join][]index.Metric
	readableJoins uint32

	readableMetrics uint32

	// we want "service-key" => Join => "service-key:val"
	// to be able to support "set" at the service-key level
	// "service-key:blue" un-sets "service-key:orange"
	// The common case is there is 1 index.Tag
	writtenTags uint32

	writtenJoins   uint32
	writtenMetrics uint32
}

func NewIndex(joinKey string) *Index {
	n := Index{
		joinKey: joinKey,
	}
	n.tagToJoin.Store(make(map[index.Tag][]Join))
	n.joinToMetric.Store(make(map[Join][]index.Metric))

	return &n
}

// Materialize copies the mutable indexes to new read-only ones and swaps out
// the existing ones.
//
// Materialize should panic in case of any problems with the data -- that
// should have been caught by validation before going into the write buffer
func (si *Index) Materialize(
	wg *sync.WaitGroup,
	joinToMetricBuffer map[Join]map[index.Metric]struct{},
	tagToJoinBuffer map[tag.ServiceKey]map[Join]index.Tag,
) {
	defer wg.Done()
	start := time.Now()
	tagToJoin := make(map[index.Tag][]Join)
	for _, joinTagPairs := range tagToJoinBuffer {
		for join, tag := range joinTagPairs {
			tagToJoin[tag] = append(tagToJoin[tag], join)
		}
	}

	for _, joinList := range tagToJoin {
		SortJoins(joinList)
	}

	totalMetrics := map[index.Metric]struct{}{}
	joinToMetric := make(map[Join][]index.Metric)
	for join, metrics := range joinToMetricBuffer {
		for metric, _ := range metrics {
			totalMetrics[metric] = struct{}{}
			joinToMetric[join] = append(joinToMetric[join], metric)
		}
	}

	for _, metricList := range joinToMetric {
		index.SortMetrics(metricList)
	}

	si.tagToJoin.Store(tagToJoin)
	si.joinToMetric.Store(joinToMetric)

	// update stats
	si.SetReadableTags(uint32(len(tagToJoin)))
	si.SetReadableJoins(uint32(len(joinToMetric)))
	si.SetReadableMetrics(uint32(len(totalMetrics)))
	si.IncrementGeneration()

	g := si.Generation()
	elapsed := time.Since(start)
	si.IncreaseGenerationTime(int64(elapsed))
	if index.Debug {
		logger.Logf("split index %s: New generation %v took %v to generate", si.Name(), g, elapsed)
	}
}

func (si *Index) Query(q *index.Query) ([]index.Metric, error) {
	// get a slice of all the join keys (for example, hostnames) associated with these tags
	tagToJoin := si.TagIndex()
	joinLists := [][]Join{}
	for _, tag := range q.Hashed {
		list, ok := tagToJoin[tag]
		if !ok {
			return []index.Metric{}, nil
		}

		joinLists = append(joinLists, list)
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
		return nil
	}

	for _, list := range joinSets {
		// any empty set --> empty intersection
		if len(list) == 0 {
			return nil
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
