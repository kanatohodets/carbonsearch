package database

import (
	"fmt"
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
	resultLimit := 10
	db := New(queryLimit, resultLimit, stats)

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
	queryLimit := 10
	resultLimit := 1
	db := New(queryLimit, resultLimit, stats)

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

func TestParseQuery(t *testing.T) {
	db := New(10, 10, stats)

	parseTagsTestCase(t, db, "basic",
		"server-state:live",
		map[string][]string{
			"server": {"server-state:live"},
		},
	)

	parseTagsTestCase(t, db, "two tags, one service",
		"server-state:live.server-hw:intel",
		map[string][]string{
			"server": {"server-state:live", "server-hw:intel"},
		},
	)

	parseTagsTestCase(t, db, "two services, one tag each",
		"server-state:live.lb-pool:www",
		map[string][]string{
			"server": {"server-state:live"},
			"lb":     {"lb-pool:www"},
		},
	)

	parseTagsTestCase(t, db, "two services, multiple tags",
		"server-state:live.server-dc:us_east.lb-pool:www.lb-weight:10",
		map[string][]string{
			"server": {"server-state:live", "server-dc:us_east"},
			"lb":     {"lb-pool:www", "lb-weight:10"},
		},
	)

	// check query size limit
	queryLimit := 1
	db = New(queryLimit, 10, stats)
	_, err := db.ParseQuery("servers-state:live.servers-dc:us_east")
	if err == nil {
		t.Errorf("oversize query failed to throw error")
		return
	}

	expectedErr := fmt.Sprintf(
		"database ParseQuery: max query size is %v, but this query has %v tags. try again with a smaller query",
		queryLimit,
		2,
	)

	if err.Error() != expectedErr {
		t.Errorf("database test: expected an error about number of tags in query, got %q instead", err)
		return
	}
}

func parseTagsTestCase(t *testing.T, db *Database, testName string, query string, expected map[string][]string) {
	parsed, err := db.ParseQuery(query)
	if err != nil {
		t.Errorf("%v parse error: %v", testName, err)
		return
	}

	for service, expectedTags := range expected {
		parsedTags, ok := parsed[service]
		if !ok {
			t.Errorf("%v: expected %v in parsed tags, but it wasn't there", testName, service)
			return
		}

		if fmt.Sprintf("%v", expectedTags) != fmt.Sprintf("%v", parsedTags) {
			t.Errorf("%v: parsed tags for %v are not what was expected! expected %v and got %v", testName, service, expectedTags, parsedTags)
			return
		}
	}

	for service, parsedTags := range parsed {
		_, ok := expected[service]
		if !ok {
			t.Errorf("%v: service %s got %v in parsed tags, but it wasn't expected", testName, service, parsedTags)
			return
		}
	}
}
