package bloom

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/index"
)

func TestQuery(t *testing.T) {
	ti := NewIndex()
	metrics := []string{
		"monitors.was_the_site_up",
		"user.messing_around_in_test",
		"monitors.nginx.http.daily",
	}
	hashed := index.HashMetrics(metrics)
	err := ti.AddMetrics(metrics, hashed)
	if err != nil {
		t.Error(err)
		return
	}

	ti.Materialize()

	q := index.NewQuery([]string{
		"text-match:nginx",
	})
	results, err := ti.Query(q)
	if err != nil {
		t.Error(err)
		return
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %v", len(results))
		return
	}

	if results[0] != hashed[2] {
		t.Errorf("expected %q in search result, got %q", hashed[2], results[0])
		return
	}

	in := NewIndex()
	metrics = []string{
		"foo",
		"bar",
		"baz",
		"blorgfoo",
		"mug_foo_ugh",
		// we don't care about positions in the bloom query, because it has no notion of positions
		"ron.crocodile.option",
		"rose.daffodil.cron",
		"popbaz",
	}
	hashes := index.HashMetrics(metrics)
	err = in.AddMetrics(metrics, hashes)
	if err != nil {
		t.Errorf("addmetrics returned an error: %v", err)
		return
	}
	err = in.Materialize()
	if err != nil {
		t.Errorf("materialize returned an error: %v", err)
		return
	}

	// bad query
	query := index.NewQuery([]string{})
	results, err = in.Query(query)
	if err == nil {
		t.Errorf("bad query got results instead of error! results: %v", results)
		return
	}

	searchTest(t, "zero results", in, []string{"qux"}, []string{})
	searchTest(t, "simple", in, []string{"foo"}, []string{"foo", "blorgfoo", "mug_foo_ugh"})
	searchTest(t, "full long metric name", in, []string{"rose.daffodil.cron"}, []string{"rose.daffodil.cron"})
	searchTest(t, "intersect, not union", in, []string{"pop", "baz"}, []string{"popbaz"})

	emptyIndex := NewIndex()
	searchTest(t, "zero results", emptyIndex, []string{"qux"}, []string{})
	searchTest(t, "simple", emptyIndex, []string{"foo"}, []string{})
}

func searchTest(t *testing.T, testName string, in *Index, searches []string, expectedResults []string) {
	tags := []string{}
	for _, search := range searches {
		tags = append(tags, "text-match:"+search)
	}

	query := index.NewQuery(tags)
	results, err := in.Query(query)
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
