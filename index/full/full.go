package full

import (
	"fmt"
	"github.com/kanatohodets/carbonsearch/index"
	"sync"
)

type Index struct {
	index      map[index.Tag][]index.Metric
	mutex      sync.RWMutex
	tagSize    int
	metricSize int
}

func NewIndex() *Index {
	return &Index{
		index: make(map[index.Tag][]index.Metric),
	}
}

func (i *Index) Add(tags []index.Tag, metrics []index.Metric) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if len(metrics) == 0 {
		return fmt.Errorf("full index: can't associate tags with 0 metrics")
	}

	if len(tags) == 0 {
		return fmt.Errorf("full index: can't associate metrics with 0 tags")
	}

	for _, tag := range tags {
		associatedMetrics, ok := i.index[tag]
		if !ok {
			i.tagSize++
			associatedMetrics = []index.Metric{}
			i.index[tag] = associatedMetrics
		}

		existingMember := make(map[index.Metric]bool)
		for _, metric := range associatedMetrics {
			existingMember[metric] = true
		}

		for _, metric := range metrics {
			_, ok := existingMember[metric]
			if !ok {
				i.metricSize++
				associatedMetrics = append(associatedMetrics, metric)
			}
		}
		index.SortMetrics(associatedMetrics)
		i.index[tag] = associatedMetrics
	}
	return nil
}

func (i *Index) Query(query []index.Tag) ([]index.Metric, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	metricSets := make([][]index.Metric, len(query))
	for pos, tag := range query {
		metricSets[pos] = i.index[tag]
	}

	return index.IntersectMetrics(metricSets), nil
}

func (i *Index) Name() string {
	return "full index"
}

func (i *Index) TagSize() int {
	// or convert i.size to an atomic
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.tagSize
}

func (i *Index) MetricSize() int {
	// or convert i.size to an atomic
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.metricSize
}
