package index

//  metricPostings is a list of documents
type metricPostings []Metric

// metricPiter is a posting list metricIterator
type metricPiter struct {
	list metricPostings
	idx  int
}

type metricTfList struct {
	freq  []int
	iters []metricIterator
}

func (tf metricTfList) Len() int { return len(tf.freq) }
func (tf metricTfList) Swap(i, j int) {
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
	tf.iters[i], tf.iters[j] = tf.iters[j], tf.iters[i]
}
func (tf metricTfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

func newMetricIter(l metricPostings) *metricPiter {
	return &metricPiter{list: l}
}

func (it *metricPiter) next() bool {
	it.idx++
	return !it.end()
}

func (it *metricPiter) advance(d Metric) bool {

	// galloping search
	bound := 1
	for it.idx+bound < len(it.list) && d > it.list[it.idx+bound] {
		bound *= 2
	}

	// inlined binary search between the last two steps
	n := d
	low, high := it.idx+bound/2, it.idx+bound
	if high > len(it.list) {
		high = len(it.list)
	}

	for low < high {
		mid := low + (high-low)/2
		if it.list[mid] >= n {
			high = mid
		} else {
			low = mid + 1
		}
	}

	it.idx = low

	return !it.end()
}

func (it *metricPiter) end() bool {
	return it.idx >= len(it.list)
}

func (it *metricPiter) at() Metric {
	return it.list[it.idx]
}

type metricIterator interface {
	at() Metric
	end() bool
	advance(Metric) bool
	next() bool
}

// intersect returns the intersection of two posting lists
// metricPostings are returned deduplicated.
func intersectMetricSetPair(result metricPostings, ait, bit metricIterator) metricPostings {

scan:
	for !ait.end() && !bit.end() {

		for ait.at() == bit.at() {

			result = append(result, bit.at())

			var d Metric

			d = ait.at()
			for ait.at() == d {
				if !ait.next() {
					break scan
				}
			}

			d = bit.at()
			for bit.at() == d {
				if !bit.next() {
					break scan
				}
			}
		}

		for ait.at() < bit.at() {
			if !ait.advance(bit.at()) {
				break scan
			}
		}

		for !bit.end() && ait.at() > bit.at() {
			if !bit.advance(ait.at()) {
				break scan
			}
		}
	}

	return result
}

// tags

//  tagPostings is a list of documents
type tagPostings []Tag

// tagPiter is a posting list tagIterator
type tagPiter struct {
	list tagPostings
	idx  int
}

type tagTfList struct {
	freq  []int
	iters []tagIterator
}

func (tf tagTfList) Len() int { return len(tf.freq) }
func (tf tagTfList) Swap(i, j int) {
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
	tf.iters[i], tf.iters[j] = tf.iters[j], tf.iters[i]
}
func (tf tagTfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

func newTagIter(l tagPostings) *tagPiter {
	return &tagPiter{list: l}
}

func (it *tagPiter) next() bool {
	it.idx++
	return !it.end()
}

func (it *tagPiter) advance(d Tag) bool {

	// galloping search
	bound := 1
	for it.idx+bound < len(it.list) && d > it.list[it.idx+bound] {
		bound *= 2
	}

	// inlined binary search between the last two steps
	n := d
	low, high := it.idx+bound/2, it.idx+bound
	if high > len(it.list) {
		high = len(it.list)
	}

	for low < high {
		mid := low + (high-low)/2
		if it.list[mid] >= n {
			high = mid
		} else {
			low = mid + 1
		}
	}

	it.idx = low

	return !it.end()
}

func (it *tagPiter) end() bool {
	return it.idx >= len(it.list)
}

func (it *tagPiter) at() Tag {
	return it.list[it.idx]
}

type tagIterator interface {
	at() Tag
	end() bool
	advance(Tag) bool
	next() bool
}

// intersect returns the intersection of two posting lists
// tagPostings are returned deduplicated.
func intersectTagSetPair(result tagPostings, ait, bit tagIterator) tagPostings {

scan:
	for !ait.end() && !bit.end() {

		for ait.at() == bit.at() {

			result = append(result, bit.at())

			var d Tag

			d = ait.at()
			for ait.at() == d {
				if !ait.next() {
					break scan
				}
			}

			d = bit.at()
			for bit.at() == d {
				if !bit.next() {
					break scan
				}
			}
		}

		for ait.at() < bit.at() {
			if !ait.advance(bit.at()) {
				break scan
			}
		}

		for !bit.end() && ait.at() > bit.at() {
			if !bit.advance(ait.at()) {
				break scan
			}
		}
	}

	return result
}
