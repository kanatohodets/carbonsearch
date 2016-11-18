package database

import (
	"reflect"
	"testing"

	"github.com/kanatohodets/carbonsearch/index/split"
)

func TestAddTag(t *testing.T) {
	toc := NewToC()
	barJoin := split.HashJoin("bar-hostname")
	toc.AddTag("foo-index", "servers", "dc", "us_west", barJoin)
	expected := map[string]map[string]map[string]map[string]int{
		"foo-index": map[string]map[string]map[string]int{
			"servers": map[string]map[string]int{
				"dc": map[string]int{
					"us_west": 0,
				},
			},
		},
	}
	table := toc.GetTable()
	if !reflect.DeepEqual(expected, table) {
		t.Errorf("table of contents after adding one tag not what was expected. expected %v, got %v", expected, table)
	}

	barMetrics := 42
	toc.SetMetricCount("foo-index", barJoin, barMetrics)
	expected["foo-index"]["servers"]["dc"]["us_west"] = barMetrics
	table = toc.GetTable()
	if !reflect.DeepEqual(expected, toc.GetTable()) {
		t.Errorf("table of contents after setting metric count not what was expected. expected %v, got %v", expected, table)
	}

	// make sure that the metric counts for different joins are summed on table generation
	quxJoin := split.HashJoin("qux-hostname")
	quxMetrics := 11
	toc.AddTag("foo-index", "servers", "dc", "us_west", quxJoin)
	toc.SetMetricCount("foo-index", quxJoin, quxMetrics)
	expected["foo-index"]["servers"]["dc"]["us_west"] = barMetrics + quxMetrics
	table = toc.GetTable()
	if !reflect.DeepEqual(expected, toc.GetTable()) {
		t.Errorf("table of contents with two joins on the same tag not what was expected. expected %v, got %v. this likely means that the metrics for different joins aren't being properly summed together to create the total metric count for a given tag", expected, table)
	}
}
