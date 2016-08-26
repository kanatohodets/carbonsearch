package text

import (
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/util/test"
	"testing"
)

func TestFilter(t *testing.T) {
	metrics := []string{
		"server.hostname-1234.cpu",
		"server.hostname-1234.mem",
		"server.hostname-1234.network",
		"server.hostname-1234.hdd",
		"monitors.conversion.still_happening",
	}

	noRegexpTags := []string{
		"server-state:live",
		"server-dc:lhr",
	}

	filtered, err := Filter(noRegexpTags, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) != len(metrics) {
		t.Errorf("a text filter with no regexp keys filtered out some metrics")
	}

	matchMetric := "monitors.conversion.still_happening"

	// exact match
	fullMatchTag := []string{
		"text-filter:" + matchMetric,
	}

	filtered, err = Filter(fullMatchTag, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) == 1 {
		if filtered[0] != matchMetric {
			t.Errorf("%v was not matched using a full match tag: %v", matchMetric, fullMatchTag)
		}
	} else {
		t.Errorf("full string match returned %d results. this is wrong because it isn't 1.", len(filtered))
	}

	// two matching tags
	twoMatchTags := []string{
		"text-filter:monitors",
		"text-filter:still_happening",
	}

	filtered, err = Filter(twoMatchTags, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) == 1 {
		if filtered[0] != matchMetric {
			t.Errorf("%v was not matched using query %s", matchMetric, twoMatchTags)
		}
	} else {
		t.Errorf("two match tag query returned %d results. this is wrong because it isn't 1.", len(filtered))
	}

	// non-matching tag
	nonMatchingTag := []string{
		"text-filter:blorg",
	}

	filtered, err = Filter(nonMatchingTag, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) != 0 {
		t.Errorf("non-matching query %v returned some results: %v", nonMatchingTag, filtered)
	}

	// two conflicting tags that each match some results, but no intersection
	conflictMatchTags := []string{
		"text-filter:monitors",
		"text-filter:server",
	}

	filtered, err = Filter(conflictMatchTags, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) != 0 {
		t.Errorf("non-matching query %v returned some results: %v", nonMatchingTag, filtered)
	}
}

func strigram(str string) trigram {
	if len(str) != 3 {
		panic("stringTrigram needs strings of length 3")
	}
	var b [3]byte
	copy(b[:], str)
	return trigramize(b)
}

func TestTokenize(t *testing.T) {
	input := "foobar"
	expected := []trigram{
		strigram("^fo"),
		strigram("foo"),
		strigram("oob"),
		strigram("oba"),
		strigram("bar"),
		strigram("ar$"),
	}

	tokens, err := tokenizeWithMarkers(input)
	if err != nil {
		t.Errorf("Tokenize returned an error: %v", err)
		return
	}

	for i, token := range tokens {
		if token != expected[i] {
			t.Errorf("tokenization of %v failed: expected %v but got %v", input, expected[i], token)
		}
	}
}

func TestAddMetrics(t *testing.T) {
	addMetricTestCase(t, "simple add a single metric", NewIndex(), []string{"foo"}, map[string]int{"foo": 1, "^fo": 1, "oo$": 1}, false)
	addMetricTestCase(t, "simple add multiple metrics", NewIndex(), []string{"foo", "bar", "baz"}, map[string]int{"foo": 1, "bar": 1, "baz": 1}, false)

	addMetricTestCase(t, "add invalid metrics (too short)", NewIndex(), []string{"fo"}, map[string]int{}, true)

	in := NewIndex()
	addMetricTestCase(t, "double-add a metric in the same batch", in, []string{"foo", "foo"}, map[string]int{"foo": 1}, false)
	addMetricTestCase(t, "add existing metric", in, []string{"foo"}, map[string]int{"foo": 1}, false)

	addMetricTestCase(t, "multiple hits for token", NewIndex(), []string{"foobar", "blorgfoo"}, map[string]int{"foo": 2, "^fo": 1, "oo$": 1}, false)
	addMetricTestCase(t, "zero hits for token", NewIndex(), []string{"foobar", "blorgfoo"}, map[string]int{"qux": 0}, false)
}

func addMetricTestCase(t *testing.T, testName string, in *Index, metrics []string, testTokens map[string]int, expectError bool) {
	hashes := index.HashMetrics(metrics)
	err := in.AddMetrics(metrics, hashes)
	if expectError {
		if err == nil {
			t.Errorf("add metrics test '%s' expected an error, but did not get one!", testName)
			return
		} else {
			return
		}
	}

	if err != nil {
		t.Errorf("add metrics test '%s' returned an error: %v", testName, err)
		return
	}

	for token, expectedCount := range testTokens {
		trig := strigram(token)
		count, err := tokenCount(in, trig)
		if err != nil {
			t.Errorf("add metrics test '%s' TokenCount for token %v returned an error: %v", testName, token, err)
			continue
		}

		if count != expectedCount {
			t.Errorf("add metrics test %v expected to find %v metrics for token %v, but instead found %v", testName, expectedCount, token, count)
		}
	}
}

func TestSearch(t *testing.T) {
	in := NewIndex()
	metrics := []string{
		"foo",
		"bar",
		"baz",
		"blorgfoo",
		"mug_foo_ugh",
		// if user query ngram order isn't respected, a search like "cron$" will return this document, since 'cro', 'ron', and 'on$' are all in this doc, just not sequential.
		"ron.crocodile.option",
		"rose.daffodil.cron",
	}
	hashes := index.HashMetrics(metrics)
	err := in.AddMetrics(metrics, hashes)
	if err != nil {
		t.Errorf("addmetrics returned an error: %v", err)
		return
	}

	// bad query
	results, err := in.Search("")
	if err == nil {
		t.Errorf("bad query got results instead of error! results: %v", results)
		return
	}

	searchTest(t, "zero results", in, "qux", []string{})
	searchTest(t, "simple", in, "foo", []string{"foo", "blorgfoo", "mug_foo_ugh"})
	searchTest(t, "start pinned", in, "^foo", []string{"foo"})
	searchTest(t, "end pinned", in, "foo$", []string{"foo", "blorgfoo"})
	searchTest(t, "start/end pinned", in, "^foo$", []string{"foo"})
	searchTest(t, "partial match but zero result", in, "^ugh", []string{})
	//searchTest(t, "respect user trigram positions", in, "cron$", []string{"rose.daffodil.cron"})

	emptyIndex := NewIndex()
	searchTest(t, "zero results", emptyIndex, "qux", []string{})
	searchTest(t, "simple", emptyIndex, "foo", []string{})
	searchTest(t, "start pinned", emptyIndex, "^foo", []string{})
	searchTest(t, "end pinned", emptyIndex, "foo$", []string{})
	searchTest(t, "start/end pinned", emptyIndex, "^foo$", []string{})
}

func searchTest(t *testing.T, testName string, in *Index, query string, expectedResults []string) {
	results, err := in.Search(query)
	if err != nil {
		t.Errorf("%s query %v returned an error: %v", testName, query, err)
		return
	}

	if len(results) != len(expectedResults) {
		t.Errorf("%s query %v returned the wrong number of things: expected %v result(s), got %v", testName, query, len(expectedResults), len(results))
		return
	}

	expectedSet := map[index.Metric]string{}
	for _, expected := range expectedResults {
		expectedSet[index.HashMetric(expected)] = expected
	}

	for _, result := range results {
		_, ok := expectedSet[result]
		if !ok {
			t.Errorf("%s query %v got an unexpected result: %v", testName, query, result)
			continue
		}
		delete(expectedSet, result)
	}

	for hash, metric := range expectedSet {
		t.Errorf("%s query %v expected to find %v (hash: %v), but it wasn't there", testName, query, metric, hash)
	}
}

func tokenCount(ti *Index, token trigram) (int, error) {
	ti.mutex.RLock()
	defer ti.mutex.RUnlock()

	post, ok := ti.postings[token]
	if !ok {
		return 0, nil
	}

	return len(post.list), nil
}

func BenchmarkAddMetrics(b *testing.B) {
	in := NewIndex()

	metricCases := make([][]string, b.N)
	hashCases := make([][]index.Metric, b.N)
	for i := 0; i < b.N; i++ {
		metricCases[i] = test.GetMetricCorpus(10)
		hashCases[i] = index.HashMetrics(metricCases[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.AddMetrics(metricCases[i], hashCases[i])
	}
}

func BenchmarkSearchWithResults(b *testing.B) {
	in := NewIndex()
	metrics := []string{
		"foo",
		"bar",
		"baz",
		"blorgfoo",
		"mug_foo_ugh",
	}
	hashes := index.HashMetrics(metrics)
	err := in.AddMetrics(metrics, hashes)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.Search("foo")
	}
}

func BenchmarkSearchWithNoResults(b *testing.B) {
	in := NewIndex()
	metrics := []string{
		"foo",
		"bar",
		"baz",
		"blorgfoo",
		"mug_foo_ugh",
	}
	hashes := index.HashMetrics(metrics)
	err := in.AddMetrics(metrics, hashes)
	if err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.Search("qux")
	}
}
