package full

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/index"
)

func TestQuery(t *testing.T) {
	metricName := "server.hostname-1234"

	hashedMetrics := index.HashMetrics([]string{metricName})
	tags := index.HashTags([]string{"server-state:live", "server-dc:lhr"})
	in := NewIndex()

	buffer := map[index.Tag]map[index.Metric]struct{}{}
	for _, tag := range tags {
		tagSet, ok := buffer[tag]
		if !ok {
			tagSet = map[index.Metric]struct{}{}
			buffer[tag] = tagSet
		}
		for _, metric := range hashedMetrics {
			tagSet[metric] = struct{}{}
		}
	}
	in.Materialize(buffer)
	query := index.NewQuery([]string{"server-state:live"})
	result, err := in.Query(query)
	if err != nil {
		t.Error(err)
	}

	if len(result) == 1 {
		if result[0] != index.HashMetric(metricName) {
			t.Errorf("full index test: %v was not found in the index", metricName)
		}
	} else {
		t.Errorf("full index test: the index had %d search results. that value is wrong because it isn't 1", len(result))
	}

	emptyResult, err := in.Query(index.NewQuery([]string{"blorgtag"}))
	if err != nil {
		t.Errorf("error querying blorgtag: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("full index text: found some results on a bogus query: %v", emptyResult)
	}

	emptyResult, err = in.Query(index.NewQuery([]string{"server-state:live", "server-dc:a_ship_in_the_ocean"}))
	if err != nil {
		t.Errorf("error querying with missing tag value: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("split index test: found some results on a partially (bad tag value) bogus query: %v", emptyResult)
	}

	emptyResult, err = in.Query(index.NewQuery([]string{"server-state:live", "server-foobar:baz"}))
	if err != nil {
		t.Errorf("error querying with missing tag key: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("split index test: found some results on a partially (tag with silly key) bogus query: %v", emptyResult)
	}
}
