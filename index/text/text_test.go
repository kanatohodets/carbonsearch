package text

import (
	"testing"
)

func TestFilter(t *testing.T) {
	metrics := []string{
		"server.hostname-1234.cpu",
		"server.hostname-1234.mem",
		"server.hostname-1234.network",
		"server.hostname-1234.hdd",
		"monitors.conversion.still_happening",
	}

	noRegexpTags := []string{
		"server-state:live",
		"server-dc:lhr",
	}

	index := NewIndex()

	filtered, err := index.Filter(noRegexpTags, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) != len(metrics) {
		t.Errorf("a text filter with no regexp keys filtered out some metrics")
	}

	matchMetric := "monitors.conversion.still_happening"

	// exact match
	fullMatchTag := []string{
		"text-filter:" + matchMetric,
	}

	filtered, err = index.Filter(fullMatchTag, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) == 1 {
		if filtered[0] != matchMetric {
			t.Errorf("%v was not matched using a full match tag: %v", matchMetric, fullMatchTag)
		}
	} else {
		t.Errorf("full string match returned %d results. this is wrong because it isn't 1.", len(filtered))
	}

	// two matching tags
	twoMatchTags := []string{
		"text-filter:^monitors",
		"text-filter:still_happening$",
	}

	filtered, err = index.Filter(twoMatchTags, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) == 1 {
		if filtered[0] != matchMetric {
			t.Errorf("%v was not matched using query %s", matchMetric, twoMatchTags)
		}
	} else {
		t.Errorf("two match tag query returned %d results. this is wrong because it isn't 1.", len(filtered))
	}

	// non-matching tag
	nonMatchingTag := []string{
		"text-filter:^blorg",
	}

	filtered, err = index.Filter(nonMatchingTag, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) != 0 {
		t.Errorf("non-matching query %v returned some results: %v", nonMatchingTag, filtered)
	}

	// two conflicting tags that each match some results, but no intersection
	conflictMatchTags := []string{
		"text-filter:^monitors",
		"text-filter:^server",
	}

	filtered, err = index.Filter(conflictMatchTags, metrics)
	if err != nil {
		t.Error(err)
	}

	if len(filtered) != 0 {
		t.Errorf("non-matching query %v returned some results: %v", nonMatchingTag, filtered)
	}
}
