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
	unionMetricTest(t, "simple 2-way union", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
	}, []string{"foo", "bar", "baz", "qux"})

	unionMetricTest(t, "simple 3-way union", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{"blorg"},
	}, []string{"foo", "bar", "baz", "qux", "blorg"})
}

func unionMetricTest(t *testing.T, testName string, rawSets [][]string, expectedResults []string) {
	metricSetFuncTest(t, testName, UnionMetrics, rawSets, expectedResults)
}

func BenchmarkUnionMetricsSmallListSmallSets(b *testing.B) {
	originalSets := make([][]Metric, 3)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10))
		SortMetrics(originalSets[i])
	}

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		UnionMetrics(copySets)
	}
}

func BenchmarkUnionMetricsSmallListLargeSets(b *testing.B) {
	originalSets := make([][]Metric, 3)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10000))
		SortMetrics(originalSets[i])
	}

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		UnionMetrics(copySets)
	}
}

func BenchmarkUnionMetricsLargeListSmallSets(b *testing.B) {
	originalSets := make([][]Metric, 300)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10))
	}

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		UnionMetrics(copySets)
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
	originalSets := make([][]Metric, 3)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10))
		SortMetrics(originalSets[i])
	}

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		IntersectMetrics(copySets)
	}
}

func BenchmarkIntersectMetricsSmallListLargeSets(b *testing.B) {
	originalSets := make([][]Metric, 3)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10000))
		SortMetrics(originalSets[i])
	}

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		IntersectMetrics(copySets)
	}
}

func BenchmarkIntersectMetricsSmallListLargeSetsOneEmpty(b *testing.B) {
	originalSets := make([][]Metric, 3)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10000))
		SortMetrics(originalSets[i])
	}
	originalSets = append(originalSets, []Metric{})

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		IntersectMetrics(copySets)
	}
}

func BenchmarkIntersectMetricsSmallListOneLargeSet(b *testing.B) {
	originalSets := make([][]Metric, 3)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(100))
		SortMetrics(originalSets[i])
	}
	originalSets = append(originalSets, HashMetrics(test.GetMetricCorpus(100000)))
	SortMetrics(originalSets[len(originalSets)-1])

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		IntersectMetrics(copySets)
	}
}

func BenchmarkIntersectMetricsLargeListSmallSets(b *testing.B) {
	originalSets := make([][]Metric, 300)
	for i, _ := range originalSets {
		originalSets[i] = HashMetrics(test.GetMetricCorpus(10))
		SortMetrics(originalSets[i])
	}

	copySets := make([][]Metric, len(originalSets))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(copySets, originalSets)
		IntersectMetrics(copySets)
	}
}

// pairwise
func BenchmarkPairwiseIntersectMetricsSmallListSmallSets(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10))
		SortMetrics(metricSets[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PairwiseIntersectMetrics(metricSets)
	}
}

func BenchmarkPairwiseIntersectMetricsSmallListLargeSets(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10000))
		SortMetrics(metricSets[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PairwiseIntersectMetrics(metricSets)
	}
}

func BenchmarkPairwiseIntersectMetricsLargeListSmallSets(b *testing.B) {
	metricSets := make([][]Metric, 300)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10))
		SortMetrics(metricSets[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PairwiseIntersectMetrics(metricSets)
	}
}

func BenchmarkPairwiseIntersectMetricsSmallListLargeSetsOneEmpty(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(10000))
		SortMetrics(metricSets[i])
	}
	metricSets = append(metricSets, []Metric{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PairwiseIntersectMetrics(metricSets)
	}
}

func BenchmarkPairwiseIntersectMetricsSmallListOneLargeSet(b *testing.B) {
	metricSets := make([][]Metric, 3)
	for i, _ := range metricSets {
		metricSets[i] = HashMetrics(test.GetMetricCorpus(100))
		SortMetrics(metricSets[i])
	}
	metricSets = append(metricSets, HashMetrics(test.GetMetricCorpus(100000)))
	SortMetrics(metricSets[len(metricSets)-1])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PairwiseIntersectMetrics(metricSets)
	}
}

func TestUnionTags(t *testing.T) {
	unionTagTest(t, "simple 2-way union", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
	}, []string{"foo", "bar", "baz", "qux"})

	unionTagTest(t, "simple 3-way union", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{"blorg"},
	}, []string{"foo", "bar", "baz", "qux", "blorg"})
}

func unionTagTest(t *testing.T, testName string, rawSets [][]string, expectedResults []string) {
	tagSetFuncTest(t, testName, UnionTags, rawSets, expectedResults)
}

func TestIntersectMetrics(t *testing.T) {
	// basic intersection, 2 sets
	intersectMetricTest(t, "basic 2-way intersection", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
	}, []string{"bar"})

	// basic intersection, 3 sets
	intersectMetricTest(t, "basic 3-way intersection", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{"blorg", "bar"},
	}, []string{"bar"})

	// empty intersection due to empty universe
	intersectMetricTest(t, "empty intersection, empty universe", [][]string{}, []string{})

	// empty intersection due to one empty subset
	intersectMetricTest(t, "empty intersection, one empty subset", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{},
	}, []string{})

	// empty intersection because nothing shared
	intersectMetricTest(t, "empty intersection, no overlap", [][]string{
		{"foo"},
		{"bar"},
		{"baz", "blorg", "buzz", "pow", "kablooie", "whizbang", "rain", "always rain"},
	}, []string{})

	// intersection with one set (yields that set)
	intersectMetricTest(t, "intersect just one item", [][]string{{"foo"}}, []string{"foo"})
}

func intersectMetricTest(t *testing.T, testName string, rawSets [][]string, expectedResults []string) {
	metricSetFuncTest(t, testName, IntersectMetrics, rawSets, expectedResults)
}

// TODO(btyler): check that the testFunc returns things in correctly sorted order
func metricSetFuncTest(t *testing.T, testName string, testFunc func([][]Metric) []Metric, rawSets [][]string, expectedResults []string) {
	mapping := map[Metric]string{}
	metricSets := make([][]Metric, len(rawSets), len(rawSets))
	for i, rawSet := range rawSets {
		metricSets[i] = make([]Metric, len(rawSet), len(rawSet))
		for j, rawMetric := range rawSet {
			hashed := HashMetric(rawMetric)
			mapping[hashed] = rawMetric
			metricSets[i][j] = hashed
		}
		SortMetrics(metricSets[i])
	}

	expectedSet := map[string]bool{}
	for _, res := range expectedResults {
		_, ok := expectedSet[res]
		if ok {
			t.Errorf("%v: '%v' appears twice in the expected result set. this is an error in the test definition", testName, res)
			return
		}
		expectedSet[res] = true
	}

	resultSet := map[string]bool{}
	for _, metric := range testFunc(metricSets) {
		str, ok := mapping[metric]
		if !ok {
			t.Errorf("%v: tried to map %v back to a string, but there was no mapping for it", testName, metric)
			return
		}

		_, ok = resultSet[str]
		if ok {
			t.Errorf("%v: '%v' appears twice in the true result set. all set functions should deduplicate.", testName, str)
			return
		}
		resultSet[str] = true
	}

	for expected, _ := range expectedSet {
		_, ok := resultSet[expected]
		if !ok {
			t.Errorf("%s: expected '%v' in metric results, but it was missing!", testName, expected)
			return
		}
	}

	for found, _ := range resultSet {
		_, ok := expectedSet[found]
		if !ok {
			t.Errorf("%s: found '%v' in metric results, but didn't expect it!", testName, found)
			return
		}
	}
}

func TestIntersectTags(t *testing.T) {
	// basic intersection, 2 sets
	intersectTagTest(t, "basic 2-way intersection", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
	}, []string{"bar"})

	// basic intersection, 3 sets
	intersectTagTest(t, "basic 3-way intersection", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{"blorg", "bar"},
	}, []string{"bar"})

	// empty intersection due to empty universe
	intersectTagTest(t, "empty intersection, empty universe", [][]string{}, []string{})

	// empty intersection due to one empty subset
	intersectTagTest(t, "empty intersection, one empty subset", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{},
	}, []string{})

	// empty intersection because nothing shared
	intersectTagTest(t, "empty intersection, no overlap", [][]string{
		{"foo"},
		{"bar"},
		{"baz", "blorg", "buzz", "pow", "kablooie", "whizbang", "rain", "always rain"},
	}, []string{})

	// intersection with one set (yields that set)
	intersectTagTest(t, "intersect just one item", [][]string{{"foo"}}, []string{"foo"})
}

func intersectTagTest(t *testing.T, testName string, rawSets [][]string, expectedResults []string) {
	tagSetFuncTest(t, testName, IntersectTags, rawSets, expectedResults)
}

// TODO(btyler): check that the testFunc returns things in correctly sorted order
func tagSetFuncTest(t *testing.T, testName string, testFunc func([][]Tag) []Tag, rawSets [][]string, expectedResults []string) {
	mapping := map[Tag]string{}
	tagSets := make([][]Tag, len(rawSets), len(rawSets))
	for i, rawSet := range rawSets {
		tagSets[i] = make([]Tag, len(rawSet), len(rawSet))
		for j, rawTag := range rawSet {
			hashed := HashTag(rawTag)
			mapping[hashed] = rawTag
			tagSets[i][j] = hashed
		}
		SortTags(tagSets[i])
	}

	expectedSet := map[string]bool{}
	for _, res := range expectedResults {
		_, ok := expectedSet[res]
		if ok {
			t.Errorf("%v: '%v' appears twice in the expected result set. this is an error in the test definition", testName, res)
			return
		}
		expectedSet[res] = true
	}

	resultSet := map[string]bool{}
	for _, tag := range testFunc(tagSets) {
		str, ok := mapping[tag]
		if !ok {
			t.Errorf("%v: tried to map %v back to a string, but there was no mapping for it", testName, tag)
			return
		}

		_, ok = resultSet[str]
		if ok {
			t.Errorf("%v: '%v' appears twice in the true result set. all set functions should deduplicate.", testName, str)
			return
		}
		resultSet[str] = true
	}

	for expected, _ := range expectedSet {
		_, ok := resultSet[expected]
		if !ok {
			t.Errorf("%s: expected '%v' in tag results, but it was missing!", testName, expected)
			return
		}
	}

	for found, _ := range resultSet {
		_, ok := expectedSet[found]
		if !ok {
			t.Errorf("%s: found '%v' in tag results, but didn't expect it!", testName, found)
			return
		}
	}
}
