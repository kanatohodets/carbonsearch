package postings

import (
	"testing"
)

func BenchmarkMaterialize(b *testing.B) {
	pi := NewIndex()
	metrics := []string{"one", "two", "three", "four", "five"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pi.Materialize(metrics)
	}
}
