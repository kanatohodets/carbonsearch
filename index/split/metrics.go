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

// Generation convenience method
func (si *Index) Generation() uint64 {
	return atomic.LoadUint64(&si.generation)
}

// IncrementGeneration incriments the generation number
func (si *Index) IncrementGeneration() {
	_ = atomic.AddUint64(&si.generation, 1)
}

// GenerationTime convenience method
func (si *Index) GenerationTime() int64 {
	return atomic.LoadInt64(&si.generationTime)
}

// IncreaseGenerationTime adds dur to the accumulated generationTime
func (si *Index) IncreaseGenerationTime(dur int64) {
	_ = atomic.AddInt64(&si.generationTime, dur)
}
