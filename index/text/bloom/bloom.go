package bloom

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/text/document"

	"github.com/dgryski/go-bloomindex"
)

type swappableBloom struct {
	mut         sync.RWMutex
	bloom       *bloomindex.Index
	docToMetric map[bloomindex.DocID]index.Metric
}

type Index struct {
	active    atomic.Value //*swappableBloom
	standby   atomic.Value //*swappableBloom
	metricMap atomic.Value //map[index.Metric]string

	numHashes int
	blockSize int
	metaSize  int
}

func NewIndex() *Index {
	ti := Index{
		//TODO(btyler): configurable?
		numHashes: 4,
		blockSize: 2048,
		metaSize:  512 * 2048,
	}

	ti.active.Store(&swappableBloom{
		bloom:       bloomindex.NewIndex(ti.blockSize, ti.metaSize, ti.numHashes),
		docToMetric: map[bloomindex.DocID]index.Metric{},
	})

	ti.standby.Store(&swappableBloom{
		bloom:       bloomindex.NewIndex(ti.blockSize, ti.metaSize, ti.numHashes),
		docToMetric: map[bloomindex.DocID]index.Metric{},
	})

	ti.metricMap.Store(map[index.Metric]string{})
	return &ti
}

func (ti *Index) Name() string {
	return "bloom text index"
}

func (ti *Index) Query(tokens []uint32) ([]index.Metric, error) {
	active := ti.Active()
	active.mut.RLock()
	defer active.mut.RUnlock()

	docIDs := active.bloom.Query(tokens)

	metrics, err := ti.docsToMetrics(active, docIDs)
	if err != nil {
		return nil, fmt.Errorf(
			"%v Query: error unmapping doc IDs: %v",
			ti.Name(),
			err,
		)
	}
	return metrics, nil
}

// TODO: active? should the docToMetric map live in the text index or internally to the backend?
func (ti *Index) docsToMetrics(active *swappableBloom, docIDs []bloomindex.DocID) ([]index.Metric, error) {
	metrics := make([]index.Metric, 0, len(docIDs))

	docMap := active.docToMetric
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

//TODO(btyler) synchronize this so it does the heavy lifting first, then waits to do atomic swap
// Materialize should panic in case of any problems with the data -- that
// should have been caught by validation before going into the write buffer
func (ti *Index) Materialize(rawMetrics []string) int {
	hashed := index.HashMetrics(rawMetrics)
	standby := ti.Standby()
	newMetricMap := map[index.Metric]string{}
	oldMetricMap := ti.MetricMap()

	// these are used to 'catch up' the formerly-active index after it goes off duty
	recentDocuments := []bloomindex.DocID{}
	recentTokens := [][]uint32{}
	recentMetrics := []index.Metric{}

	standby.mut.Lock()
	for i, rawMetric := range rawMetrics {
		metric := hashed[i]
		_, ok := oldMetricMap[metric]
		if !ok {
			tokens, err := document.Tokenize(rawMetric)
			if err != nil {
				panic(fmt.Sprintf("%s Materialize: can't tokenize %v: %v. this should have been caught by validation before adding the metric to the write buffer, hence the panic", ti.Name(), rawMetric, err))
			}
			docID := standby.bloom.AddDocument(tokens)
			recentDocuments = append(recentDocuments, docID)
			recentTokens = append(recentTokens, tokens)
			recentMetrics = append(recentMetrics, metric)

			standby.docToMetric[docID] = metric
		}
		newMetricMap[metric] = rawMetric
	}
	standby.mut.Unlock()

	// swap active/standby
	active := ti.Active()
	ti.standby.Store(active)
	ti.active.Store(standby)

	formerlyActive := ti.Standby()
	formerlyActive.mut.Lock()
	for i, docID := range recentDocuments {
		// counting on the two bloom indexes increasing document ID in
		// lockstep with each other. dgryski says this is a reasonable
		// assumption, and not breaking the encapsulation of the bloom index
		tokens := recentTokens[i]
		standbyDocID := formerlyActive.bloom.AddDocument(tokens)
		if docID != standbyDocID {
			panic("bloom index: our bloom indexes have diverged, and are not creating documents in lockstep! yikes!")
		}

		formerlyActive.docToMetric[standbyDocID] = recentMetrics[i]
	}
	formerlyActive.mut.Unlock()

	ti.metricMap.Store(newMetricMap)
	return len(newMetricMap)
}

func (ti *Index) Active() *swappableBloom {
	return ti.active.Load().(*swappableBloom)
}
func (ti *Index) Standby() *swappableBloom {
	return ti.standby.Load().(*swappableBloom)
}

func (ti *Index) MetricMap() map[index.Metric]string {
	return ti.metricMap.Load().(map[index.Metric]string)
}
