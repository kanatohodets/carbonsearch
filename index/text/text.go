package text

import (
	"regexp"
	"strings"
)

type Index struct{}

func NewIndex() *Index {
	return &Index{}
}

func (i *Index) Query(tags []string) ([]string, error) {
	return nil, nil
}

// grep as a service. this is a placeholder for having a proper text index
func (i *Index) Filter(tags []string, metrics []string) ([]string, error) {
	patterns := []string{}
	for _, tag := range tags {
		if strings.HasPrefix(tag, "text-filter:") {
			regexp := strings.TrimPrefix(tag, "text-filter:")
			patterns = append(patterns, regexp)
		}
	}

	// no regexp filters, return the whole thing
	if len(patterns) == 0 {
		return metrics, nil
	}

	matchingMetrics := map[string]int{}
	for _, pattern := range patterns {
		re, err := regexp.Compile(pattern)
		// bail out on any invalid pattern, don't partially match
		if err != nil {
			return nil, err
		}

		for _, metric := range metrics {
			matched := re.MatchString(metric)
			if matched {
				matchingMetrics[metric]++
			}
		}
	}

	result := []string{}
	for metric, count := range matchingMetrics {
		if count == len(patterns) {
			result = append(result, metric)
		}
	}

	return result, nil
}

func (i *Index) Name() string {
	return "text index"
}
