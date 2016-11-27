package database

import (
	"fmt"

	"github.com/kanatohodets/carbonsearch/index"
	"github.com/kanatohodets/carbonsearch/index/split"
	"github.com/kanatohodets/carbonsearch/tag"
)

type splitBuffer struct {
	joinToMetric map[split.Join]map[index.Metric]struct{}
	tagToJoin    map[tag.ServiceKey]map[split.Join]index.Tag
}

type writeBuffer struct {
	metrics map[string]struct{} //TODO: time.Time so things can expire out after some duration
	splits  map[string]splitBuffer
	//TODO: probably 'full' associations shouldn't be updated? that is, use index.Tag instead of tag.ServiceKey
	full map[index.Tag]map[index.Metric]struct{}
	toc  *tableOfContents
}

func NewWriteBuffer(toc *tableOfContents) *writeBuffer {
	return &writeBuffer{
		metrics: map[string]struct{}{},
		splits:  map[string]splitBuffer{},
		full:    map[index.Tag]map[index.Metric]struct{}{},
		toc:     toc,
	}
}

func (w *writeBuffer) AddSplitIndex(indexName string) error {
	_, ok := w.splits[indexName]
	if ok {
		return fmt.Errorf("database: write buffer already has entry for %v", indexName)
	}

	w.splits[indexName] = splitBuffer{
		joinToMetric: map[split.Join]map[index.Metric]struct{}{},
		tagToJoin:    map[tag.ServiceKey]map[split.Join]index.Tag{},
	}
	return nil
}

func (w *writeBuffer) BufferMetrics(indexName, rawJoin string, rawMetrics []string) error {
	splitBuffer, ok := w.splits[indexName]
	if !ok {
		return fmt.Errorf("database write buffer: no write buffer for index %q", indexName)
	}

	join := split.HashJoin(rawJoin)

	joinMetrics, ok := splitBuffer.joinToMetric[join]
	if !ok {
		joinMetrics = map[index.Metric]struct{}{}
		splitBuffer.joinToMetric[join] = joinMetrics
	}

	for _, rawMetric := range rawMetrics {
		w.metrics[rawMetric] = struct{}{}

		metric := index.HashMetric(rawMetric)
		joinMetrics[metric] = struct{}{}
	}

	w.toc.SetJoinMetricCount(indexName, join, len(joinMetrics))
	return nil
}

func (w *writeBuffer) BufferTags(indexName, rawJoin string, rawTags []string) error {
	if len(rawTags) == 0 {
		return fmt.Errorf("database write buffer: cannot add 0 tags to join %q", rawJoin)
	}

	splitBuffer, ok := w.splits[indexName]
	if !ok {
		return fmt.Errorf("database write buffer: no write buffer for index %q", indexName)
	}

	join := split.HashJoin(rawJoin)
	hashedTags := index.HashTags(rawTags)

	seenServiceKeys := map[tag.ServiceKey]string{}

	for i, rawTag := range rawTags {
		s, k, v, err := tag.Parse(rawTag)
		if err != nil {
			return fmt.Errorf("database write buffer: could not add tags to split buffer -- failure to parse tag %q: %v", rawTag, err)
		}
		sk := split.HashServiceKey(s + "-" + k)

		oldTag, ok := seenServiceKeys[sk]
		if ok {
			return fmt.Errorf("database write buffer: multiple tags (%q and %q) with key %q have been included in a batch for join %q in index %q. This is going to result in unpredictable queries for this join key (last-write-wins behavior). Bailing out on this batch.", rawTag, oldTag, s+"-"+k, rawJoin, indexName)
		}
		seenServiceKeys[sk] = rawTag
		// 'key' means something like: 'servers-status' in
		// 'servers-status:maint'. this setup is to allow changing the value from
		// 'maint' to 'live' or similar. this data structure stores the full
		// tag values (index.Tag) for a given join (think 'hostname')
		tagValueForJoins, ok := splitBuffer.tagToJoin[sk]
		if !ok {
			tagValueForJoins = map[split.Join]index.Tag{}
			splitBuffer.tagToJoin[sk] = tagValueForJoins
		}
		// if/when we want to support joins with more than one
		// value for a single tag key, we can change this to be a slice or
		// map-set of tag values. for now I think the easiest thing to reason
		// about is a single value per key.
		tagValueForJoins[join] = hashedTags[i]
		w.toc.AddJoinTag(indexName, s, k, v, join)
	}

	return nil
}

func (w *writeBuffer) BufferCustom(rawTags []string, rawMetrics []string) error {
	if len(rawMetrics) == 0 {
		return fmt.Errorf("database write buffer: can't associate tags with 0 metrics")
	}

	if len(rawTags) == 0 {
		return fmt.Errorf("database write buffer: can't associate metrics with 0 tags")
	}

	tags := index.HashTags(rawTags)
	metrics := index.HashMetrics(rawMetrics)

	for _, metric := range rawMetrics {
		w.metrics[metric] = struct{}{}
	}

	for _, tag := range tags {
		metricSet, ok := w.full[tag]
		if !ok {
			metricSet = map[index.Metric]struct{}{}
			w.full[tag] = metricSet
		}

		for _, metric := range metrics {
			w.full[tag][metric] = struct{}{}
		}
	}

	return nil
}

func (w *writeBuffer) MetricList() []string {
	list := make([]string, 0, len(w.metrics))
	for metric, _ := range w.metrics {
		list = append(list, metric)
	}
	return list
}
