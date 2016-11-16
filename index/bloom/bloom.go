package bloom

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kanatohodets/carbonsearch/index"

	"github.com/dgryski/go-bloomindex"
)

const n = 4

type Index struct {
	bloom       atomic.Value //*bloomindex.Index
	docToMetric atomic.Value //map[bloomindex.DocID]index.Metric
	metricMap   atomic.Value //map[index.Metric]string

	mut              sync.RWMutex
	mutableMetrics   map[index.Metric][]uint32
	mutableMetricMap map[index.Metric]string

	numHashes int
	blockSize int
	metaSize  int

	// reporting
	readableMetrics uint32
	writtenMetrics  uint32
	generation      uint64
}

func NewIndex() *Index {
	ti := Index{
		mutableMetrics:   map[index.Metric][]uint32{},
		mutableMetricMap: map[index.Metric]string{},

		//TODO(btyler): configurable?
		numHashes: 4,
		blockSize: 2048,
		metaSize:  512 * 2048,
	}

	ti.bloom.Store(bloomindex.NewIndex(ti.blockSize, ti.metaSize, ti.numHashes))
	ti.docToMetric.Store(map[bloomindex.DocID]index.Metric{})
	ti.metricMap.Store(map[index.Metric]string{})
	return &ti
}

func (ti *Index) Name() string {
	return "bloom text index"
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

		// thinking about it: this case is impossible because you can't have
		// dots in your query. besides, if you already know the exact metric,
		// just query graphite for that metric!
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
		searchTrigrams, err := tokenize(nonpositional)
		if err != nil {
			return nil, fmt.Errorf("%v Query: error tokenizing %v: %v", ti.Name(), search, err)
		}

		tokens = append(tokens, searchTrigrams...)
	}

	docIDs := ti.BloomIndex().Query(tokens)

	metrics, err := ti.docsToMetrics(docIDs)
	if err != nil {
		return nil, fmt.Errorf(
			"%v Query: error unmapping doc IDs: %v",
			ti.Name(),
			err,
		)
	}

	index.SortMetrics(metrics)
	return metrics, nil
}

//TODO(btyler) synchronize this so it does the heavy lifting first, then waits to do atomic swap
// alternatively keep a diff
// Materialize should panic in case of any problems with the data -- that
// should have been caught by validation before going into the write buffer
func (ti *Index) Materialize(rawMetrics []string) {
	start := time.Now()

	hashed := index.HashMetrics(rawMetrics)

	newBloom := bloomindex.NewIndex(ti.blockSize, ti.metaSize, ti.numHashes)
	docToMetric := map[bloomindex.DocID]index.Metric{}
	newMetricMap := map[index.Metric]string{}

	for i, rawMetric := range rawMetrics {
		tokens, err := tokenize(rawMetric)
		if err != nil {
			panic(fmt.Sprintf("%s Materialize: can't tokenize %v: %v. this should have been caught by validation before adding the metric to the write buffer, hence the panic", ti.Name(), rawMetric, err))
		}
		docID := newBloom.AddDocument(tokens)

		metric := hashed[i]
		docToMetric[docID] = metric
		newMetricMap[metric] = rawMetric
	}

	ti.bloom.Store(newBloom)
	ti.docToMetric.Store(docToMetric)
	ti.metricMap.Store(newMetricMap)

	// update stats
	ti.SetReadableMetrics(uint32(len(docToMetric)))
	ti.IncrementGeneration()
	g := ti.Generation()
	elapsed := time.Since(start)

	log.Printf("text index %s: New generation %v took %v to generate", ti.Name(), g, elapsed)
}

func (ti *Index) ValidateMetrics(metrics []string) []string {
	validMetrics := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		if len(metric) >= n {
			validMetrics = append(validMetrics, metric)
		}
	}
	return validMetrics
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

func (ti *Index) BloomIndex() *bloomindex.Index {
	return ti.bloom.Load().(*bloomindex.Index)
}
func (ti *Index) DocumentMap() map[bloomindex.DocID]index.Metric {
	return ti.docToMetric.Load().(map[bloomindex.DocID]index.Metric)
}
func (ti *Index) MetricMap() map[index.Metric]string {
	return ti.metricMap.Load().(map[index.Metric]string)
}

func (ti *Index) docsToMetrics(docIDs []bloomindex.DocID) ([]index.Metric, error) {
	metrics := make([]index.Metric, 0, len(docIDs))
	docMap := ti.DocumentMap()
	for _, docID := range docIDs {
		metric, ok := docMap[docID]
		if !ok {
			return nil, fmt.Errorf(
				"docsToMetrics: docID %q was missing in the docToMetric map! this is awful!",
				docID,
			)
		}
		metrics = append(metrics, metric)
	}

	return metrics, nil
}

func tokenize(term string) ([]uint32, error) {
	if len(term) < n {
		return nil, fmt.Errorf("%s is too short to search on", term)
	}

	// len(term) - 3 for quadgrams
	tokens := make([]uint32, 0, len(term)-3)
	for i := 0; i <= len(term)-n; i++ {
		var chunk [n]byte
		copy(chunk[:], term[i:i+n])
		tokens = append(tokens, ngramize(chunk))
	}

	return tokens, nil
}

func ngramize(s [n]byte) uint32 {
	return uint32(s[0])<<24 | uint32(s[1])<<16 | uint32(s[2])<<8 | uint32(s[3])
}
