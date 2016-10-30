package text

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kanatohodets/carbonsearch/index"
)

const n = 3

type trigram uint32

// number of partitions for the metrics kafka topic
const numShards = 8

type shard struct {
	postings map[trigram][]document
	mut      sync.RWMutex
}

type Index struct {
	shards [numShards]shard
	count  int
}

func NewIndex() *Index {

	var idx Index
	for i := 0; i < numShards; i++ {
		idx.shards[i].postings = map[trigram][]document{}
	}

	return &idx
}

func (ti *Index) Query(q *index.Query) ([]index.Metric, error) {
	searches := []string{}
	for _, tag := range q.Raw {
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

func (ti *Index) Name() string {
	return "text index"
}

func (ti *Index) Search(query string) ([]index.Metric, error) {
	queryTokens, err := tokenizeQuery(query)
	if err != nil {
		return nil, fmt.Errorf("text.Search: error tokenizing %v: %v", query, err)
	}

	matching := map[index.Metric][]pos{}
	skip := map[index.Metric]bool{}
	for i := 0; i < len(queryTokens); i++ {
		token := queryTokens[i]

		shard := ti.Shard(token.tri)
		shard.mut.RLock()
		docs, ok := shard.postings[token.tri]
		if !ok {
			// this trigram isn't in the index anywhere, so don't bother doing any more work: there's no match
			shard.mut.RUnlock()
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

		shard.mut.RUnlock()

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
	// this gets reused for each metric
	tokens := make([]token, 0, len(metrics[0]))
	for i, metricName := range metrics {
		if set[metricName] {
			continue
		}
		set[metricName] = true

		metric := hashes[i]
		// reset the slice length so we don't double-add anything
		tokens, err := tokenizeWithMarkers(tokens[:0], metricName)
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

	for trigram, newDocs := range trigramDelta {
		shard := ti.Shard(trigram)
		shard.mut.Lock()
		if len(shard.postings[trigram]) == 0 {
			shard.postings[trigram] = newDocs
		} else {
			union := make([]document, 0, len(shard.postings[trigram])+len(newDocs))
			shard.postings[trigram] = UnionDocuments(union, shard.postings[trigram], newDocs)
		}
		shard.mut.Unlock()
	}
	return nil
}

func (ti *Index) Shard(tri trigram) *shard {
	return &ti.shards[tri%numShards]
}
