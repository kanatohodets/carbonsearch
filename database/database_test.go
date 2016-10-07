package database

import (
	"os"
	"testing"

	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/util"
)

var stats *util.Stats

func TestMain(m *testing.M) {
	stats = util.InitStats()
	os.Exit(m.Run())
}

func TestQuery(t *testing.T) {
	queryLimit := 10
	db := New(queryLimit, stats)

	batches := []*m.TagMetric{
		{
			Tags:    []string{"custom-favorites:tester", "custom-foo:bar"},
			Metrics: []string{"monitors.was_the_site_up", "server.hostname-1234.cpu.loadavg"},
		},
		{
			Tags:    []string{"custom-dislikedByUser:jane", "custom-quux:argh"},
			Metrics: []string{"user.messing_around_in_test", "monitors.nginx.http.daily"},
		},
		{
			Tags:    []string{"custom-dislikedByUser:jane"},
			Metrics: []string{"monitors.was_the_site_up"},
		},
	}

	for _, batch := range batches {
		err := db.InsertCustom(batch)
		if err != nil {
			t.Error(err)
			return
		}
	}

	// standard query
	query := map[string][]string{
		"custom": {"custom-dislikedByUser:jane"},
	}

	result, err := db.Query(query)
	if err != nil {
		t.Error(err)
		return
	}

	expectedMetrics := map[string]bool{
		"monitors.was_the_site_up":    true,
		"user.messing_around_in_test": true,
		"monitors.nginx.http.daily":   true,
	}

	found := map[string]bool{}
	for _, metric := range result {
		found[metric] = true
		_, ok := expectedMetrics[metric]
		if !ok {
			t.Errorf("database test: found %q in the result, but we shouldn't have!", metric)
		}
	}

	for expected := range expectedMetrics {
		_, ok := found[expected]
		if !ok {
			t.Errorf("database test: expected to find %s in the query result, but it wasn't there!", expected)
		}
	}

	// zero result query
	query = map[string][]string{
		"custom": {"custom-favorites:tester", "custom-quux:arg"},
	}
	result, err = db.Query(query)
	if err != nil {
		t.Error(err)
		return
	}

	if len(result) > 0 {
		t.Errorf("database test: query that expected 0 results got: %q", result)
		return
	}

	// single result query
	query = map[string][]string{
		"custom": {"custom-favorites:tester", "custom-dislikedByUser:jane"},
	}
	result, err = db.Query(query)
	if err != nil {
		t.Error(err)
		return
	}

	if len(result) != 1 {
		t.Errorf("database test: query that expected 1 result got: %q", result)
		return
	}

	if result[0] != "monitors.was_the_site_up" {
		t.Errorf("database test: query %q expected only 'monitors.was_the_site_up', but got %q", query, result)
		return
	}
	//TODO(btyler) regex filter, split index query, intersecting between split and full index, multiple split indexes
}

func TestTooBigQuery(t *testing.T) {
	queryLimit := 1
	db := New(queryLimit, stats)

	batches := []*m.TagMetric{
		{
			Tags:    []string{"custom-favorites:tester"},
			Metrics: []string{"monitors.was_the_site_up", "server.hostname-1234.cpu.loadavg"},
		},
	}

	for _, batch := range batches {
		err := db.InsertCustom(batch)
		if err != nil {
			t.Error(err)
			return
		}
	}

	// standard query
	query := map[string][]string{
		"custom": {"custom-favorites:tester"},
	}

	results, err := db.Query(query)
	if err == nil {
		t.Errorf("database test: should have errored by claiming the query wasn't selective enough, but instead it worked and returned %q", results)
		return
	}

	if err.Error() != "database: query selected 2 metrics, which is over the limit of 1 results in a single query" {
		t.Errorf("database test: expected an error about metric result set size, got %q instead", err)
		return
	}
}

func TestInsertMetrics(t *testing.T) {

}

func TestInsertTags(t *testing.T) {

}
