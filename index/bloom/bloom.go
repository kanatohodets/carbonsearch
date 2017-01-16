package bloom

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/carbonzipper/mlog"
	"github.com/kanatohodets/carbonsearch/index"

	"github.com/dgryski/go-bloomindex"
)

const n = 4

var logger mlog.Level

type swappableBloom struct {
	mut         sync.RWMutex
	bloom       *bloomindex.Index
	docToMetric map[bloomindex.DocID]index.Metric
}

type Index struct {
	active  atomic.Value //*swappableBloom
	standby atomic.Value //*swappableBloom

	metricMap atomic.Value //map[index.Metric]string

	numHashes int
	blockSize int
	metaSize  int

	// reporting
	readableMetrics uint32
	writtenMetrics  uint32
	generation      uint64
	generationTime  int64 // time.Duration
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

	index.SortMetrics(metrics)
	return metrics, nil
}

//TODO(btyler) synchronize this so it does the heavy lifting first, then waits to do atomic swap
// Materialize should panic in case of any problems with the data -- that
// should have been caught by validation before going into the write buffer
func (ti *Index) Materialize(wg *sync.WaitGroup, rawMetrics []string) {
	start := time.Now()
	defer wg.Done()

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
			tokens, err := tokenize(rawMetric)
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

	ti.SetReadableMetrics(uint32(len(standby.docToMetric)))

	// swap active/standby
	active := ti.Active()
	ti.standby.Store(active)
	ti.active.Store(standby)

	wg.Add(1)
	go func(catchupDocuments []bloomindex.DocID, catchupTokens [][]uint32, catchupMetrics []index.Metric) {
		defer wg.Done()
		// we can be sure that this is the index we want because of the waitgroup
		formerlyActive := ti.Standby()
		formerlyActive.mut.Lock()
		for i, docID := range catchupDocuments {
			// counting on the two bloom indexes increasing document ID in
			// lockstep with each other. dgryski says this is a reasonable
			// assumption, and not breaking the encapsulation of the bloom index
			tokens := catchupTokens[i]
			standbyDocID := formerlyActive.bloom.AddDocument(tokens)
			if docID != standbyDocID {
				panic("bloom index: our bloom indexes have diverged, and are not creating documents in lockstep! yikes!")
			}

			formerlyActive.docToMetric[standbyDocID] = catchupMetrics[i]
		}
		formerlyActive.mut.Unlock()
	}(recentDocuments, recentTokens, recentMetrics)

	ti.metricMap.Store(newMetricMap)

	// update stats
	ti.IncrementGeneration()

	g := ti.Generation()
	elapsed := time.Since(start)
	ti.IncreaseGenerationTime(int64(elapsed))
	if index.Debug {
		logger.Logf("text index %s: New generation %v took %v to generate", ti.Name(), g, elapsed)
	}
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

func (ti *Index) Active() *swappableBloom {
	return ti.active.Load().(*swappableBloom)
}
func (ti *Index) MetricMap() map[index.Metric]string {
	return ti.metricMap.Load().(map[index.Metric]string)
}
func (ti *Index) Standby() *swappableBloom {
	return ti.standby.Load().(*swappableBloom)
}

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
