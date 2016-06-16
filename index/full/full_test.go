package full

import (
	"github.com/kanatohodets/carbonsearch/index"
	"testing"
)

func TestQuery(t *testing.T) {
	metricName := "server.hostname-1234"

	metrics := index.HashMetrics([]string{metricName})
	tags := index.HashTags([]string{"server-state:live", "server-dc:lhr"})
	in := NewIndex()

	in.Add(tags, metrics)
	query := index.HashTags([]string{"server-state:live"})
	result, err := in.Query(query)
	if err != nil {
		t.Error(err)
	}

	if len(result) == 1 {
		if result[0] != index.HashMetric(metricName) {
			t.Errorf("%v was not found in the index", metricName)
		}
	} else {
		t.Errorf("the index had %d search results. that value is wrong because it isn't 1", len(result))
	}

	emptyResult, err := in.Query(index.HashTags([]string{"blorgtag"}))
	if len(emptyResult) != 0 {
		t.Errorf("found some results on a bogus query: %v", emptyResult)
	}
}
