package full

import (
	"sync/atomic"
	"time"

	"github.com/dgryski/carbonzipper/mlog"

	"github.com/kanatohodets/carbonsearch/index"
)

var logger mlog.Level

type Index struct {
	index atomic.Value //map[index.Tag][]index.Metric

	// reporting
	readableTags uint32
	generation   uint64
}

func NewIndex() *Index {
	fi := &Index{}
	fi.index.Store(make(map[index.Tag][]index.Metric))

	return fi
}

func (fi *Index) Materialize(fullBuffer map[index.Tag]map[index.Metric]struct{}) error {
	start := time.Now()

	fullIndex := make(map[index.Tag][]index.Metric)
	var readableTags uint32
	for tag, metricSet := range fullBuffer {
		readableTags++
		for metric, _ := range metricSet {
			fullIndex[tag] = append(fullIndex[tag], metric)
		}
	}

	for _, metricList := range fullIndex {
		index.SortMetrics(metricList)
	}

	fi.index.Store(fullIndex)

	// update stats
	fi.SetReadableTags(readableTags)
	fi.IncrementGeneration()
	g := fi.Generation()
	elapsed := time.Since(start)
	logger.Logf("full index: New generation %v took %v to generate", g, elapsed)

	return nil
}
func (fi *Index) Query(q *index.Query) ([]index.Metric, error) {
	in := fi.Index()

	metricSets := make([][]index.Metric, len(q.Hashed))
	for pos, tag := range q.Hashed {
		metricSets[pos] = in[tag]
	}

	return index.IntersectMetrics(metricSets), nil
}

func (fi *Index) Index() map[index.Tag][]index.Metric {
	return fi.index.Load().(map[index.Tag][]index.Metric)
}

func (fi *Index) Name() string {
	return "full index"
}
