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
