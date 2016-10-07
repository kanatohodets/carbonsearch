package index

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/util"
	"github.com/kanatohodets/carbonsearch/util/test"
)

func TestHashTags(t *testing.T) {
	tags := []string{"foo", "bar"}
	hashed := HashTags(tags)
	if len(hashed) != 2 {
		t.Errorf("index test: hashed 2 tags, got %d values back", len(hashed))
		return
	}
	expected := Tag(util.HashStr64("foo"))
	if hashed[0] != expected {
		t.Errorf("index test: hashed 'foo' and expected %v, but got %v", expected, hashed[0])
	}
}

func TestHashMetrics(t *testing.T) {
	tags := []string{"foo", "bar"}
	hashed := HashMetrics(tags)
	if len(hashed) != 2 {
		t.Errorf("index test: hashed 2 metrics, got %d values back", len(hashed))
		return
	}
	expected := Metric(util.HashStr64("foo"))
	if hashed[0] != expected {
		t.Errorf("index test: hashed 'foo' and expected %v, but got %v", expected, hashed[0])
	}
}

func TestSortMetrics(t *testing.T) {
	// make sure it doesn't error on a 0 item slice
	metrics := []Metric{}
	SortMetrics(metrics)

	// 1 item
	metrics = HashMetrics([]string{"foo"})
	expectedFirst := metrics[0]
	SortMetrics(metrics)
	if metrics[0] != expectedFirst || len(metrics) > 1 {
		t.Errorf("index test: SortMetrics wrecked a 1 item slice, somehow")
		return
	}

	// create a deliberately unsorted 2 item list
	metrics = HashMetrics([]string{"foo", "bar"})
	a, b := metrics[0], metrics[1]
	expectedFirst = a
	if b > a {
		metrics = []Metric{b, a}
	} else {
		expectedFirst = b
	}

	SortMetrics(metrics)
	if metrics[0] != expectedFirst {
		t.Errorf("index test: SortMetrics did not sort the slice: expected %v as first item, but got %v", expectedFirst, metrics[0])
	}
}

func TestUnionMetrics(t *testing.T) {
	metrics := [][]Metric{
		HashMetrics([]string{"foo", "bar", "baz"}),
		HashMetrics([]string{"qux", "bar"}),
		HashMetrics([]string{"blorg"}),
	}

	for _, metricList := range metrics {
		SortMetrics(metricList)
	}

	expectedList := HashMetrics([]string{"foo", "bar", "baz", "qux", "blorg"})
	expected := map[Metric]bool{}

	for _, metric := range expectedList {
		expected[metric] = false
	}

	union := UnionMetrics(metrics)

	for _, metric := range union {
		_, ok := expected[metric]
		if !ok {
			t.Errorf("index test: metric union included %v, which was not expected", metric)
			return
		}
		expected[metric] = true
	}

	for metric, found := range expected {
		if !found {
			t.Errorf("index test: metric union did NOT include %v, which was expected to be there", metric)
		}
	}
}

func BenchmarkUnionMetricsSmallListSmallSets(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionMetrics(metricSets)
	}
}

func BenchmarkUnionMetricsSmallListLargeSets(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionMetrics(metricSets)
	}
}

func BenchmarkUnionMetricsLargeListSmallSets(b *testing.B) {
	metricSets := make([][]Metric, 300)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionMetrics(metricSets)
	}
}

/* // SLOW
func BenchmarkUnionMetricsLargeListLargeSets(b *testing.B) {
	metricSets := make([][]Metric, 300)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionMetrics(metricSets)
	}
}
*/

func BenchmarkIntersectMetricsSmallListSmallSets(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IntersectMetrics(metricSets)
	}
}

func BenchmarkIntersectMetricsSmallListLargeSets(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IntersectMetrics(metricSets)
	}
}

func BenchmarkIntersectMetricsLargeListSmallSets(b *testing.B) {
	metricSets := make([][]Metric, 300)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IntersectMetrics(metricSets)
	}
}

func TestUnionTags(t *testing.T) {
	tags := [][]Tag{
		HashTags([]string{"foo", "bar", "baz"}),
		HashTags([]string{"qux", "bar"}),
		HashTags([]string{"blorg"}),
	}

	for _, tagList := range tags {
		SortTags(tagList)
	}

	expectedList := HashTags([]string{"foo", "bar", "baz", "qux", "blorg"})
	expected := map[Tag]bool{}

	for _, tag := range expectedList {
		expected[tag] = false
	}

	union := UnionTags(tags)

	for _, tag := range union {
		_, ok := expected[tag]
		if !ok {
			t.Errorf("index test: tag union included %v, which was not expected", tag)
			return
		}
		expected[tag] = true
	}

	for tag, found := range expected {
		if !found {
			t.Errorf("index test: tag union did NOT include %v, which was expected to be there", tag)
		}
	}
}

// TODO(btyler) consolidate this into a testing table
func TestIntersectMetrics(t *testing.T) {
	// basic intersection
	metrics := [][]Metric{
		HashMetrics([]string{"foo", "bar", "baz"}),
		HashMetrics([]string{"qux", "bar"}),
		HashMetrics([]string{"blorg", "bar"}),
	}

	for _, metricList := range metrics {
		SortMetrics(metricList)
	}

	expectedList := HashMetrics([]string{"bar"})
	expected := map[Metric]bool{}

	for _, metric := range expectedList {
		expected[metric] = false
	}

	intersection := IntersectMetrics(metrics)

	for _, metric := range intersection {
		_, ok := expected[metric]
		if !ok {
			t.Errorf("index test: metric intersect included %v, which was not expected", metric)
			return
		}
		expected[metric] = true
	}

	for metric, found := range expected {
		if !found {
			t.Errorf("index test: metric intersect did NOT include %v, which was expected to be there", metric)
		}
	}

	// empty intersection due to empty universe
	intersection = IntersectMetrics([][]Metric{})
	if len(intersection) > 0 {
		t.Error("index test: metric intersect on empty set returned non-empty")
	}

	// empty intersection due to one empty subset
	metrics = [][]Metric{
		HashMetrics([]string{"foo", "bar", "baz"}),
		HashMetrics([]string{"qux", "bar"}),
		HashMetrics([]string{}),
	}

	for _, metricList := range metrics {
		SortMetrics(metricList)
	}
	intersection = IntersectMetrics(metrics)
	if len(intersection) > 0 {
		t.Error("index test: metric intersect returned non-empty, but it should have been empty")
	}

	// empty intersection due to no overlap
	metrics = [][]Metric{
		HashMetrics([]string{"foo"}),
		HashMetrics([]string{"bar"}),
		HashMetrics([]string{"baz", "blorg", "buzz", "pow", "kablooie", "whizbang", "rain", "always rain"}),
	}
	for _, metricList := range metrics {
		SortMetrics(metricList)
	}
	intersection = IntersectMetrics(metrics)
	if len(intersection) > 0 {
		t.Error("index test: metric intersect returned non-empty, but it should have been empty")
	}

	// intersection of just one thing
	metrics = [][]Metric{HashMetrics([]string{"foo"})}
	for _, metricList := range metrics {
		SortMetrics(metricList)
	}
	intersection = IntersectMetrics(metrics)
	if len(intersection) != 1 {
		t.Error("index test: metric intersect returned more than 1 result for a universe of 1")
		return
	}
	if intersection[0] != metrics[0][0] {
		t.Error("index test: somehow a universe of 1 resulted in an intersection of 1, but not that 1. wtf o_o")
		return
	}
}

// TODO(btyler) consolidate this into a testing table
func TestIntersectTags(t *testing.T) {
	// basic intersection
	tags := [][]Tag{
		HashTags([]string{"foo", "bar", "baz"}),
		HashTags([]string{"qux", "bar"}),
		HashTags([]string{"blorg", "bar"}),
	}

	for _, tagList := range tags {
		SortTags(tagList)
	}

	expectedList := HashTags([]string{"bar"})
	expected := map[Tag]bool{}

	for _, tag := range expectedList {
		expected[tag] = false
	}

	intersection := IntersectTags(tags)

	for _, tag := range intersection {
		_, ok := expected[tag]
		if !ok {
			t.Errorf("index test: tag intersect included %v, which was not expected", tag)
			return
		}
		expected[tag] = true
	}

	for tag, found := range expected {
		if !found {
			t.Errorf("index test: tag intersect did NOT include %v, which was expected to be there", tag)
		}
	}

	// empty intersection due to empty universe
	intersection = IntersectTags([][]Tag{})
	if len(intersection) > 0 {
		t.Error("index test: tag intersect on empty set returned non-empty")
	}

	// empty intersection due to one empty subset
	tags = [][]Tag{
		HashTags([]string{"foo", "bar", "baz"}),
		HashTags([]string{"qux", "bar"}),
		HashTags([]string{}),
	}

	for _, tagList := range tags {
		SortTags(tagList)
	}
	intersection = IntersectTags(tags)
	if len(intersection) > 0 {
		t.Error("index test: tag intersect returned non-empty, but it should have been empty")
	}

	// empty intersection due to no overlap
	tags = [][]Tag{
		HashTags([]string{"foo"}),
		HashTags([]string{"bar"}),
		HashTags([]string{"baz", "blorg", "buzz", "pow", "kablooie", "whizbang", "rain", "always rain"}),
	}
	for _, tagList := range tags {
		SortTags(tagList)
	}
	intersection = IntersectTags(tags)
	if len(intersection) > 0 {
		t.Error("index test: tag intersect returned non-empty, but it should have been empty")
	}

	// intersection of just one thing
	tags = [][]Tag{HashTags([]string{"foo"})}
	for _, tagList := range tags {
		SortTags(tagList)
	}
	intersection = IntersectTags(tags)
	if len(intersection) != 1 {
		t.Error("index test: tag intersect returned more than 1 result for a universe of 1")
		return
	}
	if intersection[0] != tags[0][0] {
		t.Error("index test: somehow a universe of 1 resulted in an intersection of 1, but not that 1. wtf o_o")
		return
	}
}
