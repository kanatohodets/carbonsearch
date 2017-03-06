package index

//  postings is a list of documents
type postings []Metric

const debug = false

// piter is a posting list iterator
type piter struct {
	list postings
	idx  int
}

type tfList struct {
	freq  []int
	iters []iterator
}

func (tf tfList) Len() int { return len(tf.freq) }
func (tf tfList) Swap(i, j int) {
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
	tf.iters[i], tf.iters[j] = tf.iters[j], tf.iters[i]
}
func (tf tfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

func newIter(l postings) *piter {
	return &piter{list: l}
}

func (it *piter) next() bool {
	it.idx++
	return !it.end()
}

func (it *piter) advance(d Metric) bool {

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

func (it *piter) end() bool {
	return it.idx >= len(it.list)
}

func (it *piter) at() Metric {
	return it.list[it.idx]
}

type iterator interface {
	at() Metric
	end() bool
	advance(Metric) bool
	next() bool
}

// intersect returns the intersection of two posting lists
// postings are returned deduplicated.
func intersect(result postings, ait, bit iterator) postings {

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
