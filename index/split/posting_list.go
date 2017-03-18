package split

//  joinPostings is a list of documents
type joinPostings []Join

// joinPiter is a posting list joinIterator
type joinPiter struct {
	list joinPostings
	idx  int
}

type joinTfList struct {
	freq  []int
	iters []joinIterator
}

func (tf joinTfList) Len() int { return len(tf.freq) }
func (tf joinTfList) Swap(i, j int) {
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
	tf.iters[i], tf.iters[j] = tf.iters[j], tf.iters[i]
}
func (tf joinTfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

func newJoinIter(l joinPostings) *joinPiter {
	return &joinPiter{list: l}
}

func (it *joinPiter) next() bool {
	it.idx++
	return !it.end()
}

func (it *joinPiter) advance(d Join) bool {

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

func (it *joinPiter) end() bool {
	return it.idx >= len(it.list)
}

func (it *joinPiter) at() Join {
	return it.list[it.idx]
}

type joinIterator interface {
	at() Join
	end() bool
	advance(Join) bool
	next() bool
}

// intersect returns the intersection of two posting lists
// joinPostings are returned deduplicated.
func intersectJoinSetPair(result joinPostings, ait, bit joinIterator) joinPostings {

scan:
	for !ait.end() && !bit.end() {

		for ait.at() == bit.at() {

			result = append(result, bit.at())

			var d Join

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
