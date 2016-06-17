package index

import (
	"github.com/kanatohodets/carbonsearch/util"
	"sort"
)

type Metric uint64
type MetricSlice []Metric

func (a MetricSlice) Len() int           { return len(a) }
func (a MetricSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MetricSlice) Less(i, j int) bool { return a[i] < a[j] }

type Tag uint64

type Index interface {
	Query([]Tag) ([]Metric, error)
	Name() string
}

func HashTag(tag string) Tag {
	return Tag(util.HashStr64(tag))
}

func HashTags(tags []string) []Tag {
	result := make([]Tag, len(tags))
	for i, tag := range tags {
		result[i] = HashTag(tag)
	}
	return result
}

func HashMetric(metric string) Metric {
	return Metric(util.HashStr64(metric))
}

func HashMetrics(metrics []string) []Metric {
	result := make([]Metric, len(metrics))
	for i, metric := range metrics {
		result[i] = HashMetric(metric)
	}
	return result
}

func SortMetrics(metrics []Metric) {
	sort.Sort(MetricSlice(metrics))
}
