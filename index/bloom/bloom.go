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

const n = 3

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

	return metrics, nil
}

func (ti *Index) AddMetrics(rawMetrics []string, metrics []index.Metric) error {
	if len(rawMetrics) == 0 {
		return fmt.Errorf("%s: cannot add 0 metrics to text index", ti.Name())
	}

	for i, rawMetric := range rawMetrics {
		metric := metrics[i]
		tokens, err := tokenize(rawMetric)
		if err != nil {
			return fmt.Errorf("%s AddMetrics: can't tokenize %v: %v", ti.Name(), rawMetric, err)
		}

		ti.mut.Lock()
		ti.mutableMetrics[metric] = tokens
		ti.mutableMetricMap[metric] = rawMetric
		ti.mut.Unlock()
	}

	writtenMetrics := ti.WrittenMetrics()
	ti.SetWrittenMetrics(writtenMetrics + uint32(len(rawMetrics)))
	return nil
}

func (ti *Index) Materialize() error {
	start := time.Now()

	newBloom := bloomindex.NewIndex(ti.blockSize, ti.metaSize, ti.numHashes)
	docToMetric := map[bloomindex.DocID]index.Metric{}
	newMetricMap := map[index.Metric]string{}

	// NOTE(btyler): grouping these into the same lock means we hang on to the
	// lock a bit longer, but for consistency I think it's best to always have
	// the same contents in the two
	ti.mut.RLock()
	for metric, tokens := range ti.mutableMetrics {
		docID := newBloom.AddDocument(tokens)
		docToMetric[docID] = metric
	}
	for metric, rawMetric := range ti.mutableMetricMap {
		newMetricMap[metric] = rawMetric
	}
	ti.mut.RUnlock()

	ti.bloom.Store(newBloom)
	ti.docToMetric.Store(docToMetric)
	ti.metricMap.Store(newMetricMap)

	// update stats
	ti.SetReadableMetrics(uint32(len(docToMetric)))
	ti.IncrementGeneration()
	g := ti.Generation()
	elapsed := time.Since(start)

	log.Printf("text index %s: New generation %v took %v to generate", ti.Name(), g, elapsed)
	return nil
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

	// len(term) - 1 for quadgrams
	tokens := make([]uint32, 0, len(term)-1)
	for i := 0; i <= len(term)-n; i++ {
		tokens = append(tokens, trigramize([3]byte{term[i], term[i+1], term[i+2]}))
	}

	return tokens, nil
}

func trigramize(s [3]byte) uint32 {
	return uint32(s[0])<<16 | uint32(s[1])<<8 | uint32(s[2])
}
