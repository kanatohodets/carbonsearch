package split

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/util/test"
)

func TestQuery(t *testing.T) {
	metricName := "server.hostname-1234"
	host := "hostname-1234"

	in := NewIndex("host")
	metrics := index.HashStrings([]string{metricName})
	tags := index.HashStrings([]string{"server-state:live", "server-dc:lhr"})
	query := index.NewQuery([]string{"server-state:live"})

	in.AddMetrics(host, metrics)
	in.AddTags(host, tags)
	result, err := in.Query(query)
	if err != nil {
		t.Error(err)
	}

	if len(result) == 1 {
		if result[0] != index.HashString(metricName) {
			t.Errorf("split index test: %v was not found in the index", metricName)
		}
	} else {
		t.Errorf("split index test: the index had %d search results. that value is wrong because it isn't 1", len(result))
	}

	emptyResult, err := in.Query(index.NewQuery([]string{"blorgtag"}))
	if err != nil {
		t.Errorf("error querying blorgtag: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("split index test: found some results on a bogus query: %v", emptyResult)
	}
}

func BenchmarkSmallsetQuery(b *testing.B) {
	metricName := "server.hostname-1234"
	host := "hostname-1234"
	in := NewIndex("host")
	tags := []string{"server-state:live", "server-dc:lhr"}
	metrics := index.HashStrings([]string{metricName})

	in.AddMetrics(host, metrics)
	in.AddTags(host, index.HashStrings(tags))

	query := index.NewQuery([]string{"server-state:live"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.Query(query)
	}
}

func BenchmarkLargesetQuery(b *testing.B) {
	b.StopTimer()
	in := NewIndex("host")
	hosts := test.GetJoinCorpus(100)
	queryTerms := []string{}
	for _, host := range hosts {
		in.AddMetrics(host, index.HashStrings(test.GetMetricCorpus(1000)))
		tags := test.GetTagCorpus(10)
		if test.Rand().Intn(15) == 1 {
			queryTerms = append(queryTerms, tags[test.Rand().Int()%len(tags)])
		}
		in.AddTags(host, index.HashStrings(tags))
	}

	query := index.NewQuery(queryTerms)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in.Query(query)
	}
}
