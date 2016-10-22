package text

import (
	"container/heap"
	"fmt"
	"sort"

	"github.com/kanatohodets/carbonsearch/index"
)

// we can't use uint8 because there are metrics with ~450 characters
type pos int16

type document struct {
	metric index.Hash
	pos    pos
}

type token struct {
	pos pos
	tri trigram
}

func tokenizeQuery(query string) ([]token, error) {
	tokens := []token{}
	return tokenize(query, tokens)
}

func tokenizeWithMarkers(term string) ([]token, error) {
	if len(term) < n {
		return nil, fmt.Errorf("%s is too short to search on", term)
	}

	// start with ^
	tokens := []token{
		token{
			pos(-1),
			trigramize([3]byte{'^', term[0], term[1]}),
		},
	}

	tokens, err := tokenize(term, tokens)
	if err != nil {
		return nil, err
	}

	// shove '$' on the end
	lastChar := len(term) - (n - 2)
	endMarker := trigramize([3]byte{term[lastChar-1], term[lastChar], '$'})
	tokens = append(tokens, token{pos(lastChar - 1), endMarker})

	return tokens, nil
}

func tokenize(term string, tokens []token) ([]token, error) {
	if len(term) < n {
		return nil, fmt.Errorf("%s is too short to search on", term)
	}

	for i := 0; i <= len(term)-n; i++ {
		tokens = append(
			tokens,
			token{
				pos(i),
				trigramize([3]byte{term[i], term[i+1], term[i+2]}),
			},
		)
	}

	return tokens, nil
}

func trigramize(s [3]byte) trigram {
	return trigram(uint32(s[0])<<16 | uint32(s[1])<<8 | uint32(s[2]))
}

//TODO(btyler): IntersectDocuments, UnionDocuments, SortDocuments
func SortDocuments(docs []document) {
	sort.Sort(docSlice(docs))
}

type documentSetsHeap [][]document
type docSlice []document

func (a docSlice) Len() int      { return len(a) }
func (a docSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a docSlice) Less(i, j int) bool {
	if a[i].metric == a[j].metric {
		return a[i].pos < a[j].pos
	} else {
		return a[i].metric < a[j].metric
	}
}

func (h documentSetsHeap) Len() int { return len(h) }
func (h documentSetsHeap) Less(i, j int) bool {
	if h[i][0].metric == h[j][0].metric {
		return h[i][0].pos < h[j][0].pos
	} else {
		return h[i][0].metric < h[j][0].metric
	}
}
func (h documentSetsHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *documentSetsHeap) Push(x interface{}) {
	t := x.([]document)
	*h = append(*h, t)
}

func (h *documentSetsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func UnionDocuments(docSets [][]document) []document {
	h := documentSetsHeap(docSets)
	heap.Init(&h)
	set := []document{}
	for h.Len() > 0 {
		cur := h[0]
		metric := cur[0]
		if len(set) == 0 || set[len(set)-1] != metric {
			set = append(set, metric)
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
