package full

import (
	"fmt"
	"github.com/kanatohodets/carbonsearch/index"
	"sync"
)

type Index struct {
	index      map[index.Tag]map[index.Metric]bool
	mutex      sync.RWMutex
	tagSize    int
	metricSize int
}

func NewIndex() *Index {
	return &Index{
		index: make(map[index.Tag]map[index.Metric]bool),
	}
}

func (i *Index) Add(tags []index.Tag, metrics []index.Metric) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if len(metrics) == 0 {
		return fmt.Errorf("can't associate tags with 0 metrics")
	}

	if len(tags) == 0 {
		return fmt.Errorf("can't associate metrics with 0 tags")
	}

	for _, tag := range tags {
		associatedMetrics, ok := i.index[tag]
		if !ok {
			i.tagSize++
			associatedMetrics = make(map[index.Metric]bool)
			i.index[tag] = associatedMetrics
		}
		for _, metric := range metrics {
			_, ok = associatedMetrics[metric]
			// this only needs to branch in order to avoid double-counting things
			if !ok {
				i.metricSize++
				associatedMetrics[metric] = true
			}
		}
	}
	return nil
}

func (i *Index) Query(query []index.Tag) ([]index.Metric, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	metricCounts := make(map[index.Metric]int)
	// get a slice of all the join keys (for example, hostnames) associated with these tags
	for _, tag := range query {
		// nil map -> empty range
		for metric := range i.index[tag] {
			metricCounts[metric]++
		}
	}

	var result []index.Metric
	for key, count := range metricCounts {
		if count == len(query) {
			result = append(result, key)
		}
	}

	return result, nil
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
