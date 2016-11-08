package bloom

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kanatohodets/carbonsearch/index"

	"github.com/dgryski/go-bloomindex"
)

const n = 3

type Index struct {
	bloom *bloomindex.Index

	metricSet   map[index.Metric]bool
	docToMetric map[bloomindex.DocID]index.Metric
	mut         sync.RWMutex

	count int
}

func NewIndex() *Index {
	bloom := bloomindex.NewIndex(2048, 2048*512, 4)
	return &Index{
		bloom: bloom,

		metricSet:   map[index.Metric]bool{},
		docToMetric: map[bloomindex.DocID]index.Metric{},
	}
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

	tokens := []uint32{}
	for _, query := range searches {
		queryTrigrams, err := tokenize(query)
		if err != nil {
			return nil, fmt.Errorf("%v Query: error tokenizing %v: %v", ti.Name(), query, err)
		}

		tokens = append(tokens, queryTrigrams...)
	}

	ti.mut.RLock()
	defer ti.mut.RUnlock()
	docIDs := ti.bloom.Query(tokens)

	metrics, err := ti.unmapMetrics(docIDs)
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
		ti.mut.RLock()
		_, ok := ti.metricSet[metric]
		ti.mut.RUnlock()
		// already added this metric in the past
		if ok {
			continue
		}

		tokens, err := tokenize(rawMetric)
		if err != nil {
			return fmt.Errorf("%s AddMetrics: can't tokenize %v: %v", ti.Name(), rawMetric, err)
		}

		ti.mut.Lock()
		ti.metricSet[metric] = true
		docID := ti.bloom.AddDocument([]uint32(tokens))
		ti.docToMetric[docID] = metric
		ti.mut.Unlock()
	}

	return nil
}

func (ti *Index) unmapMetrics(docIDs []bloomindex.DocID) ([]index.Metric, error) {

	metrics := make([]index.Metric, 0, len(docIDs))
	for _, docID := range docIDs {
		metric, ok := ti.docToMetric[docID]
		if !ok {
			return nil, fmt.Errorf(
				"unmapMetrics: docID %q was missing in the docToMetric map! this is awful!",
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
