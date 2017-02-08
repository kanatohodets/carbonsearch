package text

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/text/bloom"
	"github.com/kanatohodets/carbonsearch/index/text/document"

	"github.com/dgryski/carbonzipper/mlog"
)

var logger mlog.Level

/*

* refactor bloom index into an implementation of some TextIndexBackend interface or something
* once that's done and working well, implement a go-postings backend (w/ compression?)

 */

type backendType int

const (
	BloomBackend backendType = iota
)

type TextBackend interface {
	Query([]uint32) ([]index.Metric, error)
	Materialize([]string) int
	MetricMap() map[index.Metric]string
}

type Index struct {
	backend TextBackend

	// reporting
	readableMetrics uint32
	writtenMetrics  uint32
	generation      uint64
	generationTime  int64 // time.Duration
}

func NewIndex(selectedBackend backendType) *Index {
	var backend TextBackend
	switch selectedBackend {
	case BloomBackend:
		backend = bloom.NewIndex()
	default:
		panic("no backend selected for text index")
	}
	ti := Index{
		backend: backend,
	}
	return &ti
}

func (ti *Index) Query(q *index.Query) ([]index.Metric, error) {
	searches := []string{}
	for _, tag := range q.Raw {
		if strings.HasPrefix(tag, "text-match:") {
			search := strings.TrimPrefix(tag, "text-match:")
			searches = append(searches, search)
		}
	}

	if len(searches) == 0 {
		return nil, fmt.Errorf("%v Query: no text searches in query: %v", ti.Name(), q.Raw)
	}

	tokens := []uint32{}
	for _, search := range searches {
		nonpositional := strings.Trim(search, "^$")
		searchTrigrams, err := document.Tokenize(nonpositional)
		if err != nil {
			return nil, fmt.Errorf("%v Query: error tokenizing %v: %v", ti.Name(), search, err)
		}

		tokens = append(tokens, searchTrigrams...)
	}

	metrics, err := ti.backend.Query(tokens)
	if err != nil {
		return nil, fmt.Errorf("%v Query: error querying text backend: %v", ti.Name(), err)
	}

	index.SortMetrics(metrics)
	return metrics, nil
}

// Filter filters a set of string metrics using a set of text tags
// (text-match:foobar). Returns the string metrics which match all of the given
// text tags.
func (ti *Index) Filter(textTags, metrics []string) []string {
	matches := []string{}
	intersectionCounts := make([]int, len(metrics))
	for _, tag := range textTags {
		search := strings.TrimPrefix(tag, "text-match:")
		// broken pin -> no possible matches -> empty intersection
		if search[0] == '$' || search[len(search)-1] == '^' {
			return []string{}
		}
		caret := search[0] == '^'
		dollar := search[len(search)-1] == '$'
		nonpositional := strings.Trim(search, "^$")

		// TODO: maybe this is slow? map based intersect and such
		// this case is a little silly, since you should probably just query
		// graphite for that metric directly
		if caret && dollar {
			for i, rawMetric := range metrics {
				if rawMetric == nonpositional {
					intersectionCounts[i]++
					if intersectionCounts[i] == len(textTags) {
						matches = append(matches, rawMetric)
					}
				}
			}
		} else if caret {
			for i, rawMetric := range metrics {
				if strings.HasPrefix(rawMetric, nonpositional) {
					intersectionCounts[i]++
					if intersectionCounts[i] == len(textTags) {
						matches = append(matches, rawMetric)
					}
				}
			}
		} else if dollar {
			for i, rawMetric := range metrics {
				if strings.HasSuffix(rawMetric, nonpositional) {
					intersectionCounts[i]++
					if intersectionCounts[i] == len(textTags) {
						matches = append(matches, rawMetric)
					}
				}
			}
		} else {
			for i, rawMetric := range metrics {
				if strings.Contains(rawMetric, nonpositional) {
					intersectionCounts[i]++
					if intersectionCounts[i] == len(textTags) {
						matches = append(matches, rawMetric)
					}
				}
			}
		}
	}

	return matches
}

//TODO(btyler) synchronize this so it does the heavy lifting first, then waits to do atomic swap
// Materialize should panic in case of any problems with the data -- that
// should have been caught by validation before going into the write buffer
func (ti *Index) Materialize(wg *sync.WaitGroup, rawMetrics []string) {
	start := time.Now()
	defer wg.Done()

	readableMetrics := ti.backend.Materialize(rawMetrics)
	ti.SetReadableMetrics(uint32(readableMetrics))

	// update stats
	ti.IncrementGeneration()

	g := ti.Generation()
	elapsed := time.Since(start)
	ti.IncreaseGenerationTime(int64(elapsed))
	if index.Debug {
		logger.Logf("text index %s: New generation %v took %v to generate", ti.Name(), g, elapsed)
	}
}

func (ti *Index) Name() string {
	return "text index"
}

// UnmapMetrics converts typed []uint64 metrics to string
func (ti *Index) UnmapMetrics(metrics []index.Metric) ([]string, error) {
	metricMap := ti.MetricMap()
	rawMetrics := make([]string, 0, len(metrics))

	for _, metric := range metrics {
		raw, ok := metricMap[metric]
		if !ok {
			return nil, fmt.Errorf("text index: the hashed metric '%d' has no mapping back to a string! this is awful", metric)
		}
		rawMetrics = append(rawMetrics, raw)
	}

	return rawMetrics, nil
}

func (ti *Index) MetricMap() map[index.Metric]string {
	return ti.backend.MetricMap()
}
