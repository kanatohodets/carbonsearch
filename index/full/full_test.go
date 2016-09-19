package full

import (
	"testing"
)

func TestQuery(t *testing.T) {
	metricName := "server.hostname-1234"

	index := NewIndex()

	index.Add([]string{"server-state:live", "server-dc:lhr"}, []string{metricName})
	result, err := index.Query([]string{"server-state:live"})
	if err != nil {
		t.Error(err)
	}

	if len(result) == 1 {
		if result[0] != metricName {
			t.Errorf("%v was not found in the index", metricName)
		}
	} else {
		t.Errorf("the index had %d search results. that value is wrong because it isn't 1", len(result))
	}

	emptyResult, err := index.Query([]string{"blorgtag"})
	if len(emptyResult) != 0 {
		t.Errorf("found some results on a bogus query: %v", emptyResult)
	}
}
