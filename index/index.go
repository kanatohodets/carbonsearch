package index

import (
	"container/heap"
	"sort"

	"github.com/kanatohodets/carbonsearch/util"
)

type Hash uint64
type HashSlice []Hash

func (a HashSlice) Len() int           { return len(a) }
func (a HashSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a HashSlice) Less(i, j int) bool { return a[i] < a[j] }

type Query struct {
	Raw  []string
	Tags []Hash
}

func NewQuery(raw []string) *Query {
	tags := HashStrings(raw)
	return &Query{
		Raw:  raw,
		Tags: tags,
	}
}

//TODO(btyler) -- think about whether this should dedupe
func (q *Query) AddTags(raw []string) {
	tags := HashStrings(raw)
	q.Raw = append(q.Raw, raw...)
	q.Tags = append(q.Tags, tags...)
}

type Index interface {
	Query(*Query) ([]Hash, error)
	Name() string
}

func HashString(item string) Hash {
	return Hash(util.HashStr64(item))
}

func HashStrings(items []string) []Hash {
	result := make([]Hash, len(items))
	for i, item := range items {
		result[i] = HashString(item)
	}
	return result
}

func SortHashes(hashes []Hash) {
	sort.Sort(HashSlice(hashes))
}

type HashSetsHeap [][]Hash

func (h HashSetsHeap) Len() int           { return len(h) }
func (h HashSetsHeap) Less(i, j int) bool { return h[i][0] < h[j][0] }
func (h HashSetsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *HashSetsHeap) Push(x interface{}) {
	t := x.([]Hash)
	*h = append(*h, t)
}

func (h *HashSetsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func UnionHashes(hashSets [][]Hash) []Hash {
	h := HashSetsHeap(hashSets)
	heap.Init(&h)
	set := []Hash{}
	for h.Len() > 0 {
		cur := h[0]
		hash := cur[0]
		if len(set) == 0 || set[len(set)-1] != hash {
			set = append(set, hash)
		}
		if len(cur) == 1 {
			heap.Pop(&h)
		} else {
			h[0] = cur[1:]
			heap.Fix(&h, 0)
		}
	}
	return set
}

func IntersectHashes(hashedSets [][]Hash) []Hash {
	if len(hashedSets) == 0 {
		return []Hash{}
	}

	for _, list := range hashedSets {
		// any empty set --> empty intersection
		if len(list) == 0 {
			return []Hash{}
		}
	}

	h := HashSetsHeap(hashedSets)
	heap.Init(&h)
	set := []Hash{}
	for {
		cur := h[0]
		smallestHash := cur[0]
		present := 0
		for _, candidate := range h {
			if candidate[0] == smallestHash {
				present++
			} else {
				// any further matches will be purged by the fixup loop
				break
			}
		}

		// found something in every subset
		if present == len(hashedSets) {
			if len(set) == 0 || set[len(set)-1] != smallestHash {
				set = append(set, smallestHash)
			}
		}

		for h[0][0] == smallestHash {
			list := h[0]
			if len(list) == 1 {
				return set
			}

			h[0] = list[1:]
			heap.Fix(&h, 0)
		}
	}
}
