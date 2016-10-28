package split

import (
	"sync/atomic"
)

// Tags
func (si *Index) ReadableTags() uint32 {
	return atomic.LoadUint32(&si.readableTags)
}

func (si *Index) SetReadableTags(new uint32) {
	_ = atomic.SwapUint32(&si.readableTags, new)
}

func (si *Index) WrittenTags() uint32 {
	return atomic.LoadUint32(&si.writtenTags)
}

func (si *Index) SetWrittenTags(new uint32) {
	_ = atomic.SwapUint32(&si.writtenTags, new)
}

// Metrics
func (si *Index) ReadableMetrics() uint32 {
	return atomic.LoadUint32(&si.readableMetrics)
}

func (si *Index) SetReadableMetrics(new uint32) {
	_ = atomic.SwapUint32(&si.readableMetrics, new)
}

func (si *Index) WrittenMetrics() uint32 {
	return atomic.LoadUint32(&si.writtenMetrics)
}

func (si *Index) SetWrittenMetrics(new uint32) {
	_ = atomic.SwapUint32(&si.writtenMetrics, new)
}

// Joins
func (si *Index) ReadableJoins() uint32 {
	return atomic.LoadUint32(&si.readableJoins)
}

func (si *Index) SetReadableJoins(new uint32) {
	_ = atomic.SwapUint32(&si.readableJoins, new)
}

func (si *Index) WrittenJoins() uint32 {
	return atomic.LoadUint32(&si.writtenJoins)
}

func (si *Index) SetWrittenJoins(new uint32) {
	_ = atomic.SwapUint32(&si.writtenJoins, new)
}

// Generation
func (si *Index) Generation() uint64 {
	return atomic.LoadUint64(&si.generation)
}

func (si *Index) IncrementGeneration() {
	newGen := uint64(si.Generation() + 1)
	_ = atomic.SwapUint64(&si.generation, newGen)
}
