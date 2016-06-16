package split

import (
	"github.com/kanatohodets/carbonsearch/index"
	"math/rand"
	"testing"
)

var seed int64 = 232342358902345
var rnd *rand.Rand

func TestQuery(t *testing.T) {
	metricName := "server.hostname-1234"
	host := "hostname-1234"

	in := NewIndex("host")
	metrics := index.HashMetrics([]string{metricName})
	tags := index.HashTags([]string{"server-state:live", "server-dc:lhr"})
	query := index.HashTags([]string{"server-state:live"})

	in.AddMetrics(host, metrics)
	in.AddTags(host, tags)
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

func BenchmarkSmallsetQuery(b *testing.B) {
	metricName := "server.hostname-1234"
	host := "hostname-1234"
	in := NewIndex("host")
	tags := []string{"server-state:live", "server-dc:lhr"}
	metrics := index.HashMetrics([]string{metricName})

	in.AddMetrics(host, metrics)
	in.AddTags(host, index.HashTags(tags))

	query := index.HashTags([]string{"server-state:live"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.Query(query)
	}
}

func BenchmarkLargesetQuery(b *testing.B) {
	b.StopTimer()
	rnd = rand.New(rand.NewSource(seed))
	in := NewIndex("host")
	hosts := rwords(100, 40)
	queryTerms := []string{}
	for _, host := range hosts {
		in.AddMetrics(host, index.HashMetrics(rwords(5000, 100)))
		tags := rwords(10, 30)
		if rnd.Intn(15) == 1 {
			queryTerms = append(queryTerms, tags[rnd.Int()%len(tags)])
		}
		in.AddTags(host, index.HashTags(tags))
	}

	query := index.HashTags(queryTerms)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		in.Query(query)
	}
}

var alpha string = "abcdefghijklmnopqrstuvwxyz"

func rwords(n int, wordMaxLen int) []string {
	words := make([]string, n)
	for i := 0; i < n; i++ {
		l := rnd.Intn(wordMaxLen) + 1
		word := make([]byte, l)
		for j := 0; j < l; j++ {
			word = append(word, rchr())
		}
		words = append(words, string(word))
	}
	return words
}

func rchr() byte {
	return alpha[rnd.Int()%26]
}
