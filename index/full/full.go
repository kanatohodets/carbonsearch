package full

import (
	"fmt"
	"sync"

	"github.com/kanatohodets/carbonsearch/index"
)

type Index struct {
	index      map[index.Hash][]index.Hash
	mutex      sync.RWMutex
	tagSize    int
	metricSize int
}

func NewIndex() *Index {
	return &Index{
		index: make(map[index.Hash][]index.Hash),
	}
}

func (fi *Index) Add(tags []index.Hash, metrics []index.Hash) error {
	fi.mutex.Lock()
	defer fi.mutex.Unlock()

	if len(metrics) == 0 {
		return fmt.Errorf("full index: can't associate tags with 0 metrics")
	}

	if len(tags) == 0 {
		return fmt.Errorf("full index: can't associate metrics with 0 tags")
	}

	for _, tag := range tags {
		associatedMetrics, ok := fi.index[tag]
		if !ok {
			fi.tagSize++
			associatedMetrics = []index.Hash{}
			fi.index[tag] = associatedMetrics
		}

		existingMember := make(map[index.Hash]bool)
		for _, metric := range associatedMetrics {
			existingMember[metric] = true
		}

		for _, metric := range metrics {
			_, ok := existingMember[metric]
			if !ok {
				fi.metricSize++
				associatedMetrics = append(associatedMetrics, metric)
			}
		}
		index.SortHashes(associatedMetrics)
		fi.index[tag] = associatedMetrics
	}
	return nil
}

func (fi *Index) Query(q *index.Query) ([]index.Hash, error) {
	fi.mutex.RLock()
	defer fi.mutex.RUnlock()

	metricSets := make([][]index.Hash, len(q.Tags))
	for pos, tag := range q.Tags {
		metricSets[pos] = fi.index[tag]
	}

	return index.IntersectHashes(metricSets), nil
}

func (fi *Index) Name() string {
	return "full index"
}

func (fi *Index) TagSize() int {
	// or convert fi.size to an atomic
	fi.mutex.RLock()
	defer fi.mutex.RUnlock()
	return fi.tagSize
}

func (fi *Index) MetricSize() int {
	// or convert fi.size to an atomic
	fi.mutex.RLock()
	defer fi.mutex.RUnlock()
	return fi.metricSize
}
