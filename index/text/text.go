package text

import (
	"github.com/kanatohodets/carbonsearch/index"
	"strings"
)

type Index struct{}

func NewIndex() *Index {
	return &Index{}
}

func (i *Index) Query(tags []index.Tag) ([]index.Metric, error) {
	return nil, nil
}

// grep as a service. this is a placeholder for having a proper text index
func (i *Index) Filter(tags []string, metrics []string) ([]string, error) {
	substrs := []string{}
	for _, tag := range tags {
		if strings.HasPrefix(tag, "text-filter:") {
			substr := strings.TrimPrefix(tag, "text-filter:")
			substrs = append(substrs, substr)
		}
	}

	// no substr filters, return the whole thing
	if len(substrs) == 0 {
		return metrics, nil
	}

	matchingMetrics := map[string]int{}
	for _, substr := range substrs {
		for _, metric := range metrics {
			if strings.Contains(metric, substr) {
				matchingMetrics[metric]++
			}
		}
	}

	result := []string{}
	for metric, count := range matchingMetrics {
		if count == len(substrs) {
			result = append(result, metric)
		}
	}

	return result, nil
}

func (i *Index) Name() string {
	return "text index"
}
