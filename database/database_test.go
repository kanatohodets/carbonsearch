package database

import (
	"fmt"
	"os"
	"testing"

	m "github.com/kanatohodets/carbonsearch/consumer/message"
	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/util"
)

var stats *util.Stats

func TestMain(m *testing.M) {
	stats = util.InitStats()
	os.Exit(m.Run())
}

var queryLimit = 10
var resultLimit = 10
var fullService = "custom"
var textService = "text"
var splitIndexes = map[string]string{
	"fqdn": "servers",
}

func TestFullQuery(t *testing.T) {
	db := New(queryLimit, resultLimit, fullService, textService, splitIndexes, stats)

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

	err := db.MaterializeIndexes()
	if err != nil {
		t.Error(err)
		return
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

func populateSplitIndex(t *testing.T, db *Database, testName, joinKey string, data map[string]map[string][]string) {
	for joinValue, splitSides := range data {
		metricMsg := &m.KeyMetric{
			Key:     joinKey,
			Value:   joinValue,
			Metrics: splitSides["metrics"],
		}

		tagMsg := &m.KeyTag{
			Key:   joinKey,
			Value: joinValue,
			Tags:  splitSides["tags"],
		}

		err := db.InsertMetrics(metricMsg)
		if err != nil {
			t.Errorf("%v: problem inserting metrics: %v", testName, err)
			return
		}

		err = db.InsertTags(tagMsg)
		if err != nil {
			t.Errorf("%v: problem inserting tags: %v", testName, err)
			return
		}
	}

	err := db.MaterializeIndexes()
	if err != nil {
		t.Errorf("%v problem materializing indexes: %v", testName, err)
		return
	}
}

func queryTest(t *testing.T, db *Database, testName string, query string, expectedMetrics []string) {
	parsedQuery, err := db.ParseQuery(query)
	if err != nil {
		t.Errorf("%v: error parsing query (this is not what this test is testing, so probably a buggy test): %v", testName, err)
		return
	}

	expectedSet := map[string]bool{}
	for _, metric := range expectedMetrics {
		expectedSet[metric] = true
	}

	result, err := db.Query(parsedQuery)

	resultSet := map[string]bool{}
	for _, metric := range result {
		resultSet[metric] = true
		_, ok := expectedSet[metric]
		if !ok {
			t.Errorf("%v: found %q in the result, but we shouldn't have!", testName, metric)
			return
		}
	}

	for expected := range expectedSet {
		_, ok := resultSet[expected]
		if !ok {
			t.Errorf("%v: expected to find %q in the query result, but it wasn't there!", testName, expected)
			return
		}
	}

	// we don't do this for text queries because they're filtered at the string
	// level, so sortedness is no longer a viable (or important) property to
	// maintain
	if len(parsedQuery["text"]) == 0 {
		// ensure results are sorted. breaking this is a symptom that some of the
		// sets are stored unsorted at some point, which breaks a boatload of
		// assumptions
		expectedHashes := index.HashMetrics(expectedMetrics)
		index.SortMetrics(expectedHashes)

		resultHashes := index.HashMetrics(result)
		if fmt.Sprintf("%v", expectedHashes) != fmt.Sprintf("%v", resultHashes) {
			t.Errorf("%v: expected and result metrics are the same, but in a different order!", testName)
			logger.Logf("%v expected: %q, %q", testName, expectedMetrics, expectedHashes)
			logger.Logf("%v result: %q, %q", testName, result, resultHashes)

			return
		}
	}
}

func TestSplitQuery(t *testing.T) {
	db := New(queryLimit, resultLimit, fullService, textService, splitIndexes, stats)

	populateSplitIndex(t, db, "basic split queries",
		"fqdn",
		map[string]map[string][]string{
			"foohost-4335.staging.example.com": map[string][]string{
				"metrics": []string{
					// the ordering of these two is important: they expose a bug if metrics are not sorted on ingest
					"server.foohost-4335_staging_example_com.cpu.loadavg",
					"monitors.was_the_site_up",
				},
				"tags": []string{
					"servers-hw:shiny",
					"servers-dc:us_west",
					"servers-status:live",
					"servers-roles:foo",
				},
			},
			"barhost-1000.prod.example.com": map[string][]string{
				"metrics": []string{
					"server.barhost-1000_prod_example_com.tcp.tx_byte",
				},
				"tags": []string{
					"servers-hw:shiny",
					"servers-dc:us_west",
					"servers-status:live",
					"servers-roles:bar",
				},
			},
			"quxhost-0003.dev.example.com": map[string][]string{
				"metrics": []string{
					"server.quxhost-0003_dev_example_com.iowait.5m",
				},
				"tags": []string{
					"servers-hw:rusty",
					"servers-dc:us_east",
					"servers-status:borked",
					"servers-roles:qux",
				},
			},
		},
	)
	splitIndexQueryTests(t, db, "first generation")

	// regenerate index adding nothing
	populateSplitIndex(t, db, "second generation",
		"fqdn",
		map[string]map[string][]string{},
	)
	splitIndexQueryTests(t, db, "second generation, empty index add")

	// regenerate index adding different stuff
	populateSplitIndex(t, db, "third generation",
		"fqdn",
		map[string]map[string][]string{
			"qux-03.prod.example.com": map[string][]string{
				"metrics": []string{
					"server.barhost-1000_prod_example_com.tcp.tx_byte",
				},
				"tags": []string{
					"servers-dc:us_west",
					"servers-status:live",
				},
			},
		},
	)
	splitIndexQueryTests(t, db, "third generation, add things")

	// regenerate + retest many times over: every generation iterates over the maps in question, and thus is a chance for missing sorts to be caught
	for i := 0; i < 25; i++ {
		populateSplitIndex(t, db, fmt.Sprintf("mega generation %v", i),
			"fqdn",
			map[string]map[string][]string{},
		)
		splitIndexQueryTests(t, db, fmt.Sprintf("looking for broken ordering in generation %v", i))
	}

	//TODO(btyler) regex filter, split index query, intersecting between split and full index, multiple split indexes
}

func TestTextQuery(t *testing.T) {
	var unusedSplitIndexes = map[string]string{
		"foobar_unused_key": "foobar_unused_service",
	}

	db := New(queryLimit, resultLimit, fullService, textService, unusedSplitIndexes, stats)

	err := db.InsertMetrics(&m.KeyMetric{
		Key:   "foobar_unused_key",
		Value: "foobar_unused_value",
		Metrics: []string{
			"foo",
			"bar",
			"baz",
			"blorgfoo",
			"mug_foo_ugh",
			// if user query ngram order isn't respected, a search like "cron$"
			// will return this document, since 'cro', 'ron', and 'on$' are all in
			// this doc, just not sequential.
			"ron.crocodile.option",
			"rose_daffodil_cron",
			"popbaz",
			"bazpop",
		}})

	if err != nil {
		t.Errorf("Text Query: problem inserting metrics: %v", err)
		return
	}

	err = db.MaterializeIndexes()
	if err != nil {
		t.Errorf("materialize returned an error: %v", err)
		return
	}

	searchTest(t, db, "zero results", []string{"qux"}, []string{})
	searchTest(t, db, "simple", []string{"foo"}, []string{"foo", "blorgfoo", "mug_foo_ugh"})
	searchTest(t, db, "start pinned", []string{"^foo"}, []string{"foo"})
	searchTest(t, db, "end pinned", []string{"foo$"}, []string{"foo", "blorgfoo"})
	searchTest(t, db, "start/end pinned", []string{"^foo$"}, []string{"foo"})
	searchTest(t, db, "partial match but zero result", []string{"^ugh"}, []string{})
	searchTest(t, db, "respect user trigram positions", []string{"cron$"}, []string{"rose_daffodil_cron"})
	searchTest(t, db, "full long metric name", []string{"rose_daffodil_cron"}, []string{"rose_daffodil_cron"})
	searchTest(t, db, "full long metric name pinned", []string{"^rose_daffodil_cron$"}, []string{"rose_daffodil_cron"})
	searchTest(t, db, "full long metric name, but broken pins", []string{"$rose_daffodil_cron^"}, []string{})

	searchTest(t, db, "text filter intersects, not unions", []string{"^pop", "baz"}, []string{"popbaz"})
}

func searchTest(t *testing.T, db *Database, testName string, searches, expected []string) {
	queryString := "text-match:" + searches[0]
	for i, search := range searches {
		if i == 0 {
			continue
		}
		queryString = queryString + ".text-match:" + search
	}
	queryTest(t, db, fmt.Sprintf("text test: %v", testName), queryString, expected)
}

func splitIndexQueryTests(t *testing.T, db *Database, prefix string) {
	queryTest(t, db, fmt.Sprintf("%v single tag query, one split index", prefix),
		"servers-dc:us_west",
		[]string{
			"server.barhost-1000_prod_example_com.tcp.tx_byte",
			"monitors.was_the_site_up",
			"server.foohost-4335_staging_example_com.cpu.loadavg",
		},
	)

	queryTest(t, db, fmt.Sprintf("%v zero result query", prefix),
		"custom-foo:bar",
		[]string{},
	)

	queryTest(t, db, fmt.Sprintf("%v single result query", prefix),
		"servers-status:live",
		[]string{
			"server.barhost-1000_prod_example_com.tcp.tx_byte",
			"server.foohost-4335_staging_example_com.cpu.loadavg",
			"monitors.was_the_site_up",
		},
	)

	queryTest(t, db, fmt.Sprintf("%v single result query", prefix),
		"servers-status:live.servers-hw:shiny.servers-dc:us_west",
		[]string{
			"server.barhost-1000_prod_example_com.tcp.tx_byte",
			"server.foohost-4335_staging_example_com.cpu.loadavg",
			"monitors.was_the_site_up",
		},
	)
}

func TestTooVagueQuery(t *testing.T) {
	smallResultLimit := 1
	db := New(queryLimit, smallResultLimit, fullService, textService, splitIndexes, stats)

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

	err := db.MaterializeIndexes()
	if err != nil {
		t.Error(err)
		return
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
	db := New(queryLimit, resultLimit, fullService, textService, splitIndexes, stats)

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
	smallQueryLimit := 1
	db = New(smallQueryLimit, resultLimit, fullService, textService, splitIndexes, stats)
	_, err := db.ParseQuery("servers-state:live.servers-dc:us_east")
	if err == nil {
		t.Errorf("oversize query failed to throw error")
		return
	}

	expectedErr := fmt.Sprintf(
		"database ParseQuery: max query size is %v, but this query has %v tags. try again with a smaller query",
		smallQueryLimit,
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
