package text

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kanatohodets/carbonsearch/index"
)

var n int = 3

type trigram uint32

type Index struct {
	postings map[trigram][]document
	mutex    sync.RWMutex
	count    int
}

func NewIndex() *Index {
	return &Index{
		postings: make(map[trigram][]document),
	}
}

func (ti *Index) Query(rawTags []string) ([]index.Metric, error) {
	searches := []string{}
	for _, tag := range rawTags {
		if strings.HasPrefix(tag, "text-match:") {
			search := strings.TrimPrefix(tag, "text-match:")
			searches = append(searches, search)
		}
	}
	metricSets := make([][]index.Metric, len(searches))
	for i, search := range searches {
		results, err := ti.Search(search)
		if err != nil {
			return nil, fmt.Errorf("text index query: error while searching string %v: %v", search, err)
		}
		metricSets[i] = results
	}
	return index.IntersectMetrics(metricSets), nil
}

func (i *Index) Name() string {
	return "text index"
}

func (ti *Index) Search(query string) ([]index.Metric, error) {
	ti.mutex.RLock()
	defer ti.mutex.RUnlock()
	queryTokens, err := tokenizeQuery(query)
	if err != nil {
		return nil, fmt.Errorf("text.Search: error tokenizing %v: %v", query, err)
	}

	matching := map[index.Metric][]pos{}
	skip := map[index.Metric]bool{}
	for i := 0; i < len(queryTokens); i++ {
		token := queryTokens[i]
		docs, ok := ti.postings[token.tri]
		if !ok {
			// this trigram isn't in the index anywhere, so don't bother doing any more work: there's no match
			return []index.Metric{}, nil
		}

		// pick out documents with a matching position
		for _, doc := range docs {
			if skip[doc.metric] {
				continue
			}

			matching[doc.metric] = append(matching[doc.metric], doc.pos)
			list := matching[doc.metric]
			// query: foo$ -> 0 'foo', 1 'oo$': the result list should always be i+1
			// metric ^bfoo$ -> 0 ^bf, 1 bfo, 2 foo, 3 oo$.
			// first match is 'foo', followed by 'oo$', so list should be [2, 3]
			// previous match should be directly preceding this one
			if len(list) != i+1 || (len(list) > 1 && list[i] != list[i-1]+1) {
				skip[doc.metric] = true
			}
		}

		if len(skip) == len(matching) {
			return []index.Metric{}, nil
		}

	}

	res := make([]index.Metric, 0, len(matching)-len(skip))
	for metric, hits := range matching {
		if skip[metric] || len(hits) != len(queryTokens) {
			continue
		}
		res = append(res, metric)
	}

	index.SortMetrics(res)

	return res, nil
}

func (ti *Index) AddMetrics(metrics []string, hashes []index.Metric) error {
	if len(metrics) == 0 {
		return fmt.Errorf("text index: cannot add 0 metrics to text index")
	}

	trigramDelta := map[trigram][]document{}
	set := map[string]bool{}
	for i, metricName := range metrics {
		if set[metricName] {
			continue
		}
		set[metricName] = true

		metric := hashes[i]
		tokens, err := tokenizeWithMarkers(metricName)
		if err != nil {
			return fmt.Errorf("text index: could not tokenize %v: %v", metricName, err)
		}

		for _, token := range tokens {
			trigramDelta[token.tri] = append(trigramDelta[token.tri], document{metric, token.pos})
		}
	}

	for _, docs := range trigramDelta {
		SortDocuments(docs)
	}

	ti.mutex.Lock()
	defer ti.mutex.Unlock()
	for trigram, newDocs := range trigramDelta {
		docs := ti.postings[trigram]
		if len(docs) == 0 {
			ti.postings[trigram] = newDocs
		} else {
			ti.postings[trigram] = UnionDocuments([][]document{ti.postings[trigram], newDocs})
		}
	}
	return nil
}
