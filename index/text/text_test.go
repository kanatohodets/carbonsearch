package text

import (
	"sync"
	"testing"

	"github.com/kanatohodets/carbonsearch/index"
)

var testBackend backendType = BloomBackend

func TestQuery(t *testing.T) {
	ti := NewIndex(testBackend)
	metrics := []string{
		"monitors.was_the_site_up",
		"user.messing_around_in_test",
		"monitors.nginx.http.daily",
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	ti.Materialize(wg, metrics)
	wg.Wait()

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

	hashed := index.HashMetrics(metrics)
	if results[0] != hashed[2] {
		t.Errorf("expected %q in search result, got %q", hashed[2], results[0])
		return
	}

	in := NewIndex(testBackend)
	metrics = []string{
		"foox",
		"bart",
		"bazz",
		"blorgfoox",
		"mug_foox_ugh",
		// we don't care about positions in the bloom query, because it has no notion of positions
		"ron.crocodile.option",
		"rose.daffodil.cron",
		"kpopbazz",
	}

	wg = &sync.WaitGroup{}
	wg.Add(1)
	in.Materialize(wg, metrics)
	wg.Wait()

	// bad query
	query := index.NewQuery([]string{})
	results, err = in.Query(query)
	if err == nil {
		t.Errorf("bad query got results instead of error! results: %v", results)
		return
	}

	searchTest(t, "zero results", in, []string{"quxx"}, []string{})
	searchTest(t, "simple", in, []string{"foox"}, []string{"foox", "blorgfoox", "mug_foox_ugh"})
	searchTest(t, "full long metric name", in, []string{"rose.daffodil.cron"}, []string{"rose.daffodil.cron"})
	searchTest(t, "intersect, not union", in, []string{"kpop", "bazz"}, []string{"kpopbazz"})

	emptyIndex := NewIndex(testBackend)
	searchTest(t, "zero results", emptyIndex, []string{"quxx"}, []string{})
	searchTest(t, "simple", emptyIndex, []string{"foox"}, []string{})
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
