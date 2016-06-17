package split

import (
	"github.com/kanatohodets/carbonsearch/index"
	"math/rand"
	"testing"
)

var seed int64 = 232342358902345
var rnd *rand.Rand

func TestSortJoins(t *testing.T) {
	// make sure it doesn't error on a 0 item slice
	joins := []Join{}
	SortJoins(joins)

	// 1 item
	joins = []Join{HashJoin("foo")}
	expectedFirst := joins[0]
	SortJoins(joins)
	if joins[0] != expectedFirst || len(joins) > 1 {
		t.Errorf("index test: SortJoins wrecked a 1 item slice, somehow")
		return
	}

	// create a deliberately unsorted 2 item list
	joins = []Join{
		HashJoin("foo"),
		HashJoin("bar"),
	}
	a, b := joins[0], joins[1]
	expectedFirst = a
	if b > a {
		joins = []Join{b, a}
	} else {
		expectedFirst = b
	}

	SortJoins(joins)
	if joins[0] != expectedFirst {
		t.Errorf("index test: SortJoins did not sort the slice: expected %v as first item, but got %v", expectedFirst, joins[0])
	}

}

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
			t.Errorf("split index test: %v was not found in the index", metricName)
		}
	} else {
		t.Errorf("split index test: the index had %d search results. that value is wrong because it isn't 1", len(result))
	}

	emptyResult, err := in.Query(index.HashTags([]string{"blorgtag"}))
	if len(emptyResult) != 0 {
		t.Errorf("split index test: found some results on a bogus query: %v", emptyResult)
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
