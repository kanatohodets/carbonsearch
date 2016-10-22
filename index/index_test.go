package index

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/util/test"
)

func TestSortHashes(t *testing.T) {
	// make sure it doesn't error on a 0 item slice
	hashes := []Hash{}
	SortHashes(hashes)

	// 1 item
	hashes = HashStrings([]string{"foo"})
	expectedFirst := hashes[0]
	SortHashes(hashes)
	if hashes[0] != expectedFirst || len(hashes) > 1 {
		t.Errorf("index test: SortHashes wrecked a 1 item slice, somehow")
		return
	}

	// create a deliberately unsorted 2 item list
	hashes = HashStrings([]string{"foo", "bar"})
	a, b := hashes[0], hashes[1]
	expectedFirst = a
	if b > a {
		hashes = []Hash{b, a}
	} else {
		expectedFirst = b
	}

	SortHashes(hashes)
	if hashes[0] != expectedFirst {
		t.Errorf("index test: SortHashes did not sort the slice: expected %v as first item, but got %v", expectedFirst, hashes[0])
	}
}

func BenchmarkUnionSmallListSmallSets(b *testing.B) {
	hashSets := make([][]Hash, 3)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionHashes(hashSets)
	}
}

func BenchmarkUnionSmallListLargeSets(b *testing.B) {
	hashSets := make([][]Hash, 3)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionHashes(hashSets)
	}
}

func BenchmarkUnionHashesLargeListSmallSets(b *testing.B) {
	hashSets := make([][]Hash, 300)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionHashes(hashSets)
	}
}

/* // SLOW
func BenchmarkUnionHashesLargeListLargeSets(b *testing.B) {
	hashSets := make([][]Hash, 300)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnionHashes(hashSets)
	}
}
*/

func BenchmarkIntersectHashesSmallListSmallSets(b *testing.B) {
	hashSets := make([][]Hash, 3)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IntersectHashes(hashSets)
	}
}

func BenchmarkIntersectHashesSmallListLargeSets(b *testing.B) {
	hashSets := make([][]Hash, 3)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IntersectHashes(hashSets)
	}
}

func BenchmarkIntersectHashesLargeListSmallSets(b *testing.B) {
	hashSets := make([][]Hash, 300)
	for i, _ := range hashSets {
		hashSets[i] = HashStrings(test.GetMetricCorpus(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IntersectHashes(hashSets)
	}
}

func TestUnion(t *testing.T) {
	unionTest(t, "simple 2-way union", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
	}, []string{"foo", "bar", "baz", "qux"})

	unionTest(t, "simple 3-way union", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{"blorg"},
	}, []string{"foo", "bar", "baz", "qux", "blorg"})
}

func unionTest(t *testing.T, testName string, rawSets [][]string, expectedResults []string) {
	setFuncTest(t, testName, UnionHashes, rawSets, expectedResults)
}

func TestIntersect(t *testing.T) {
	// basic intersection, 2 sets
	intersectTest(t, "basic 2-way intersection", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
	}, []string{"bar"})

	// basic intersection, 3 sets
	intersectTest(t, "basic 3-way intersection", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{"blorg", "bar"},
	}, []string{"bar"})

	// empty intersection due to empty universe
	intersectTest(t, "empty intersection, empty universe", [][]string{}, []string{})

	// empty intersection due to one empty subset
	intersectTest(t, "empty intersection, one empty subset", [][]string{
		{"foo", "bar", "baz"},
		{"qux", "bar"},
		{},
	}, []string{})

	// empty intersection because nothing shared
	intersectTest(t, "empty intersection, no overlap", [][]string{
		{"foo"},
		{"bar"},
		{"baz", "blorg", "buzz", "pow", "kablooie", "whizbang", "rain", "always rain"},
	}, []string{})

	// intersection with one set (yields that set)
	intersectTest(t, "intersect just one item", [][]string{{"foo"}}, []string{"foo"})
}

func intersectTest(t *testing.T, testName string, rawSets [][]string, expectedResults []string) {
	setFuncTest(t, testName, IntersectHashes, rawSets, expectedResults)
}

// TODO(btyler): check that the testFunc returns things in correctly sorted order
func setFuncTest(t *testing.T, testName string, testFunc func([][]Hash) []Hash, rawSets [][]string, expectedResults []string) {
	mapping := map[Hash]string{}
	hashSets := make([][]Hash, len(rawSets), len(rawSets))
	for i, rawSet := range rawSets {
		hashSets[i] = make([]Hash, len(rawSet), len(rawSet))
		for j, rawItem := range rawSet {
			hashed := HashString(rawItem)
			mapping[hashed] = rawItem
			hashSets[i][j] = hashed
		}
		SortHashes(hashSets[i])
	}

	expectedSet := map[string]bool{}
	for _, res := range expectedResults {
		_, ok := expectedSet[res]
		if ok {
			t.Errorf("%v: '%v' appears twice in the expected result set. this is an error in the test definition", testName, res)
			return
		}
		expectedSet[res] = true
	}

	resultSet := map[string]bool{}
	for _, hash := range testFunc(hashSets) {
		str, ok := mapping[hash]
		if !ok {
			t.Errorf("%v: tried to map %v back to a string, but there was no mapping for it", testName, hash)
			return
		}

		_, ok = resultSet[str]
		if ok {
			t.Errorf("%v: '%v' appears twice in the true result set. all set functions should deduplicate.", testName, str)
			return
		}
		resultSet[str] = true
	}

	for expected, _ := range expectedSet {
		_, ok := resultSet[expected]
		if !ok {
			t.Errorf("%s: expected '%v' in results, but it was missing!", testName, expected)
			return
		}
	}

	for found, _ := range resultSet {
		_, ok := expectedSet[found]
		if !ok {
			t.Errorf("%s: found '%v' in results, but didn't expect it!", testName, found)
			return
		}
	}
}
