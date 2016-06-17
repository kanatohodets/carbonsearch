package index

import (
	"github.com/kanatohodets/carbonsearch/util"
	"testing"
)

func TestHashTags(t *testing.T) {
	tags := []string{"foo", "bar"}
	hashed := HashTags(tags)
	if len(hashed) != 2 {
		t.Errorf("index test: hashed 2 tags, got %d values back", len(hashed))
		return
	}
	expected := Tag(util.HashStr64("foo"))
	if hashed[0] != expected {
		t.Errorf("index test: hashed 'foo' and expected %v, but got %v", expected, hashed[0])
	}
}

func TestHashMetrics(t *testing.T) {
	tags := []string{"foo", "bar"}
	hashed := HashMetrics(tags)
	if len(hashed) != 2 {
		t.Errorf("index test: hashed 2 metrics, got %d values back", len(hashed))
		return
	}
	expected := Metric(util.HashStr64("foo"))
	if hashed[0] != expected {
		t.Errorf("index test: hashed 'foo' and expected %v, but got %v", expected, hashed[0])
	}
}

func TestSortMetrics(t *testing.T) {
	// make sure it doesn't error on a 0 item slice
	metrics := []Metric{}
	SortMetrics(metrics)

	// 1 item
	metrics = HashMetrics([]string{"foo"})
	expectedFirst := metrics[0]
	SortMetrics(metrics)
	if metrics[0] != expectedFirst || len(metrics) > 1 {
		t.Errorf("index test: SortMetrics wrecked a 1 item slice, somehow")
		return
	}

	// create a deliberately unsorted 2 item list
	metrics = HashMetrics([]string{"foo", "bar"})
	a, b := metrics[0], metrics[1]
	expectedFirst = a
	if b > a {
		metrics = []Metric{b, a}
	} else {
		expectedFirst = b
	}

	SortMetrics(metrics)
	if metrics[0] != expectedFirst {
		t.Errorf("index test: SortMetrics did not sort the slice: expected %v as first item, but got %v", expectedFirst, metrics[0])
	}
}
