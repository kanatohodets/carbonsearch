package text

import (
	"fmt"
	"github.com/kanatohodets/carbonsearch/index"
	"strings"
	"sync"
)

type posting struct {
	count int
	list  []index.Metric
}

var n int = 3

type trigram uint32

type Index struct {
	postings map[trigram]*posting
	mutex    sync.RWMutex
	count    int
}

func NewIndex() *Index {
	return &Index{
		postings: make(map[trigram]*posting),
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

// grep as a service. this is a placeholder for having a proper text index
func Filter(tags []string, metrics []string) ([]string, error) {
	substrs := []string{}
	for _, tag := range tags {
		if strings.HasPrefix(tag, "text-filter:") {
			substr := strings.TrimPrefix(tag, "text-filter:")
			substrs = append(substrs, substr)
		}
	}

	// no substr filters, return the whole thing
	if len(substrs) == 0 {
		return metrics, nil
	}

	matchingMetrics := map[string]int{}
	for _, substr := range substrs {
		for _, metric := range metrics {
			if strings.Contains(metric, substr) {
				matchingMetrics[metric]++
			}
		}
	}

	result := []string{}
	for metric, count := range matchingMetrics {
		if count == len(substrs) {
			result = append(result, metric)
		}
	}

	return result, nil
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

	metricSets := [][]index.Metric{}
	for _, token := range queryTokens {
		post, ok := ti.postings[token]
		if !ok {
			// this trigram isn't in the index anywhere, so don't bother doing any more work: there's no match
			return []index.Metric{}, nil
		}
		metricSets = append(metricSets, post.list)
	}

	return index.IntersectMetrics(metricSets), nil
}

func (ti *Index) AddMetrics(metrics []string, hashes []index.Metric) error {
	if len(metrics) == 0 {
		return fmt.Errorf("text index: cannot add 0 metrics to text index")
	}

	tokenDelta := map[trigram][]index.Metric{}
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
			tokenDelta[token] = append(tokenDelta[token], metric)
		}
	}

	ti.mutex.Lock()
	defer ti.mutex.Unlock()
	for token, newList := range tokenDelta {
		post, ok := ti.postings[token]
		if !ok {
			post = &posting{}
			ti.postings[token] = post
		}

		exists := map[index.Metric]bool{}
		for _, existingMetric := range post.list {
			exists[existingMetric] = true
		}

		changed := false
		for _, new := range newList {
			if exists[new] {
				continue
			}

			post.count++
			changed = true
			post.list = append(post.list, new)
		}

		if changed {
			index.SortMetrics(post.list)
		}
	}
	return nil
}

func tokenizeQuery(query string) ([]trigram, error) {
	tokens := []trigram{}
	return tokenize(query, tokens)
}

func tokenizeWithMarkers(term string) ([]trigram, error) {
	if len(term) < n {
		return nil, fmt.Errorf("%s is too short to search on", term)
	}

	// start with ^
	tokens := []trigram{
		trigramize([3]byte{'^', term[0], term[1]}),
	}

	tokens, err := tokenize(term, tokens)
	if err != nil {
		return nil, err
	}

	// shove '$' on the end
	end := trigramize(
		[3]byte{
			term[len(term)-(n-1)],
			term[len(term)-(n-2)],
			'$',
		},
	)
	tokens = append(tokens, end)
	return tokens, nil
}

func tokenize(term string, tokens []trigram) ([]trigram, error) {
	if len(term) < n {
		return nil, fmt.Errorf("%s is too short to search on", term)
	}

	for i := 0; i <= len(term)-n; i++ {
		tokens = append(
			tokens,
			trigramize([3]byte{term[i], term[i+1], term[i+2]}),
		)
	}

	return tokens, nil
}

func trigramize(s [3]byte) trigram {
	return trigram(uint32(s[0])<<16 | uint32(s[1])<<8 | uint32(s[2]))
}
