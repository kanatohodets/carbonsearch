package postings

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/text/document"

	"github.com/dgryski/go-postings"
)

type Index struct {
	idx       atomic.Value //*postings.Index
	docMetric atomic.Value //map[postings.DocID]index.Metric
	metricMap atomic.Value //map[index.Metric]string
}

func NewIndex() *Index {
	pi := &Index{}

	pi.idx.Store(postings.NewIndex(nil))
	pi.metricMap.Store(map[index.Metric]string{})
	pi.docMetric.Store(map[postings.DocID]index.Metric{})

	return pi
}

func (pi *Index) Name() string {
	return "postinglist text index"
}

func (pi *Index) Query(tokens []uint32) ([]index.Metric, error) {
	docIDs := postings.Query(
		pi.index(),
		unsafeTermIDSlice(tokens),
	)

	metrics, err := pi.docsToMetrics(docIDs)
	if err != nil {
		return nil, fmt.Errorf(
			"%v Query: error unmapping doc IDs: %v",
			pi.Name(),
			err,
		)
	}

	return metrics, nil
}

func (pi *Index) Materialize(rawMetrics []string) int {
	newIdx := postings.NewIndex(nil)
	newDocToMetric := map[postings.DocID]index.Metric{}
	newMetricMap := map[index.Metric]string{}

	hashed := index.HashMetrics(rawMetrics)
	for i, rawMetric := range rawMetrics {
		metric := hashed[i]
		tokens, err := document.Tokenize(rawMetric)
		if err != nil {
			panic(fmt.Sprintf("%v: cannot tokenize %q: %v", pi.Name(), metric, err))
		}
		docID := newIdx.AddDocument(unsafeTermIDSlice(tokens))
		newDocToMetric[docID] = metric
		newMetricMap[metric] = rawMetric
	}

	pi.idx.Store(newIdx)
	pi.docMetric.Store(newDocToMetric)
	pi.metricMap.Store(newMetricMap)

	return len(rawMetrics)
}

// this is because currently it is impossible to convert []uint32 to []TermID (where 'type TermID uint32')
func unsafeTermIDSlice(v []uint32) []postings.TermID {
	return *(*[]postings.TermID)(unsafe.Pointer(&v))
}

func (pi *Index) MetricMap() map[index.Metric]string {
	return pi.metricMap.Load().(map[index.Metric]string)
}

func (pi *Index) index() *postings.Index {
	return pi.idx.Load().(*postings.Index)
}

func (pi *Index) docToMetricMap() map[postings.DocID]index.Metric {
	return pi.docMetric.Load().(map[postings.DocID]index.Metric)
}

func (pi *Index) docsToMetrics(docIDs postings.Postings) ([]index.Metric, error) {
	metrics := make([]index.Metric, 0, len(docIDs))

	docMap := pi.docToMetricMap()
	for _, docID := range docIDs {
		metric, ok := docMap[docID]
		if !ok {
			return nil, fmt.Errorf(
				"%s docsToMetrics: docID %q was missing in the docToMetric map! this is awful!",
				pi.Name(),
				docID,
			)
		}
		metrics = append(metrics, metric)
	}

	return metrics, nil
}
