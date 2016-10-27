package text

import (
	"fmt"
	"sort"

	"github.com/kanatohodets/carbonsearch/index"
)

// we can't use uint8 because there are metrics with ~450 characters
type pos int16

type document struct {
	metric index.Metric
	pos    pos
}

type token struct {
	pos pos
	tri trigram
}

func tokenizeQuery(query string) ([]token, error) {
	return tokenize(nil, query)
}

func tokenizeWithMarkers(tokens []token, term string) ([]token, error) {
	if len(term) < n {
		return nil, fmt.Errorf("%s is too short to search on", term)
	}

	// start with ^
	tokens = append(tokens, token{
		pos(-1),
		trigramize([3]byte{'^', term[0], term[1]}),
	})

	tokens, err := tokenize(tokens, term)
	if err != nil {
		return nil, err
	}

	// shove '$' on the end
	lastChar := len(term) - (n - 2)
	endMarker := trigramize([3]byte{term[lastChar-1], term[lastChar], '$'})
	tokens = append(tokens, token{pos(lastChar - 1), endMarker})

	return tokens, nil
}

func tokenize(tokens []token, term string) ([]token, error) {
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

//TODO(btyler): IntersectDocuments
func SortDocuments(docs []document) {
	sort.Sort(docSlice(docs))
}

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

// lightly modified from github.com/dgryski/go-trigram to be a deduplicating
// union for documents (metric + position)
func UnionDocuments(result, a, b []document) []document {
	var aidx, bidx int

scan:
	for aidx < len(a) && bidx < len(b) {
		if a[aidx].metric == b[bidx].metric {
			if len(result) == 0 {
				result = append(result, a[aidx])
			} else {
				last := result[len(result)-1]
				if a[aidx].metric != last.metric || (a[aidx].metric == last.metric && a[aidx].pos != last.pos) {
					result = append(result, a[aidx])
				}
			}

			aidx++
			bidx++
			if aidx == len(a) || bidx == len(b) {
				break scan
			}
		}

		for a[aidx].metric < b[bidx].metric {
			if len(result) == 0 {
				result = append(result, a[aidx])
			} else {
				last := result[len(result)-1]
				if a[aidx].metric != last.metric || (a[aidx].metric == last.metric && a[aidx].pos != last.pos) {
					result = append(result, a[aidx])
				}
			}

			aidx++
			if aidx == len(a) {
				break scan
			}
		}

		for a[aidx].metric > b[bidx].metric {
			if len(result) == 0 {
				result = append(result, b[bidx])
			} else {
				last := result[len(result)-1]
				if b[bidx].metric != last.metric || (b[bidx].metric == last.metric && b[bidx].pos != last.pos) {
					result = append(result, b[bidx])
				}
			}

			bidx++
			if bidx == len(b) {
				break scan
			}
		}
	}
	// we may have broken out because we either finished b, or a, or both
	// processes any remainders
	for aidx < len(a) {
		result = append(result, a[aidx])
		aidx++
	}

	for bidx < len(b) {
		result = append(result, b[bidx])
		bidx++
	}

	return result
}
