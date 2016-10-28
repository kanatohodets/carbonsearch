package text

import (
	"fmt"
	"log"
	"testing"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/util/test"
)

type StringDoc struct {
	metric string
	pos    int
}

var globalBenchmarkResult []document

func TestUnionDocuments(t *testing.T) {
	unionDocTest(t, "basic union", [2][]StringDoc{
		{{"foobar", 0}},
		{{"qux", 0}},
	}, []StringDoc{
		{"qux", 0},
		{"foobar", 0},
	})
}

func unionDocTest(t *testing.T, testName string, stringDocSets [2][]StringDoc, expectedResults []StringDoc) {
	reverse := map[document]StringDoc{}
	docSets := [][]document{}
	for _, stringSet := range stringDocSets {
		docs := []document{}
		for _, strDoc := range stringSet {
			doc := document{
				index.HashMetric(strDoc.metric),
				pos(strDoc.pos),
			}
			docs = append(docs, doc)
			reverse[doc] = strDoc
		}
		SortDocuments(docs)
		docSets = append(docSets, docs)
	}

	expected := []document{}
	expectedSet := map[document]bool{}
	for _, strDoc := range expectedResults {
		doc := document{
			index.HashMetric(strDoc.metric),
			pos(strDoc.pos),
		}

		expected = append(expected, doc)
		expectedSet[doc] = true
		reverse[doc] = strDoc
	}

	SortDocuments(expected)

	result := UnionDocuments(nil, docSets[0], docSets[1])
	resultSet := map[document]bool{}
	for _, res := range result {
		resultSet[res] = true
	}

	for expectedDoc, _ := range expectedSet {
		strDoc, ok := reverse[expectedDoc]
		if !ok {
			t.Errorf("%s: one of the expected set isn't present in the reverse hash. this is an error in the test setup", testName)
			return
		}

		_, ok = resultSet[expectedDoc]
		if !ok {
			t.Errorf("%s: expected %v in the result set, but it was not there", testName, strDoc)
			log.Printf("%s: expected %v", testName, expected)
			log.Printf("%s: result %v", testName, result)
			return
		}
	}

	for found, _ := range resultSet {
		strDoc, ok := reverse[found]
		if !ok {
			t.Errorf("%s: one of the result set isn't present in the reverse hash. very weird...", testName)
			return
		}

		_, ok = expectedSet[found]
		if !ok {
			t.Errorf("%s: found %v in the result set, but it was not expected!", testName, strDoc)
			return
		}
	}

	if fmt.Sprintf("%v", expected) != fmt.Sprintf("%v", result) {
		t.Errorf("%s: expected and result sets are mis-aligned!: expected: %v vs result: %v", testName, expected, result)
	}
}

func BenchmarkUnionDocuments(b *testing.B) {
	docSets := [2][]document{}
	for i := 0; i < 2; i++ {
		set := []document{}
		metrics := index.HashMetrics(test.GetMetricCorpus(10))
		positions := test.GetDocumentPositions(10)
		for i, metric := range metrics {
			set = append(set, document{
				metric,
				pos(positions[i]),
			})
		}
		SortDocuments(set)
		docSets[i] = set
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		globalBenchmarkResult = UnionDocuments(nil, docSets[0], docSets[1])
	}
}

func BenchmarkUnevenUnionDocuments(b *testing.B) {
	bigSet := []document{}
	metrics := index.HashMetrics(test.GetMetricCorpus(10000))
	positions := test.GetDocumentPositions(10000)
	for i, metric := range metrics {
		bigSet = append(bigSet, document{
			metric,
			pos(positions[i]),
		})
	}
	SortDocuments(bigSet)

	smallSet := []document{}
	metrics = index.HashMetrics(test.GetMetricCorpus(10))
	positions = test.GetDocumentPositions(10)
	for i, metric := range metrics {
		smallSet = append(smallSet, document{
			metric,
			pos(positions[i]),
		})
	}
	SortDocuments(smallSet)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		globalBenchmarkResult = UnionDocuments(nil, bigSet, smallSet)
	}
}
