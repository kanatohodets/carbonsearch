package bloom

import (
	"testing"

	"github.com/kanatohodets/carbonsearch/index"
)

func TestQuery(t *testing.T) {
	ti := NewIndex()
	metrics := []string{
		"monitors.was_the_site_up",
		"user.messing_around_in_test",
		"monitors.nginx.http.daily",
	}
	hashed := index.HashMetrics(metrics)
	err := ti.AddMetrics(metrics, hashed)
	if err != nil {
		t.Error(err)
		return
	}

	ti.Materialize()

	q := index.NewQuery([]string{
		"text-match:nginx",
	})
	results, err := ti.Query(q)
	if err != nil {
		t.Error(err)
		return
	}

	if len(results) != 1 {
		t.Errorf("expected 1 result, got %v", len(results))
		return
	}

	if results[0] != hashed[2] {
		t.Errorf("expected %q in search result, got %q", hashed[2], results[0])
		return
	}
}
