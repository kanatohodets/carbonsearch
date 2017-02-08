package text

import (
	"sync/atomic"
)

// Metrics
func (ti *Index) ReadableMetrics() uint32 {
	return atomic.LoadUint32(&ti.readableMetrics)
}

func (ti *Index) SetReadableMetrics(new uint32) {
	_ = atomic.SwapUint32(&ti.readableMetrics, new)
}

func (ti *Index) WrittenMetrics() uint32 {
	return atomic.LoadUint32(&ti.writtenMetrics)
}

func (ti *Index) SetWrittenMetrics(new uint32) {
	_ = atomic.SwapUint32(&ti.writtenMetrics, new)
}

// Generation convenience method
func (ti *Index) Generation() uint64 {
	return atomic.LoadUint64(&ti.generation)
}

// IncrementGeneration incriments the generation number
func (ti *Index) IncrementGeneration() {
	_ = atomic.AddUint64(&ti.generation, 1)
}

// GenerationTime convenience method
func (ti *Index) GenerationTime() int64 {
	return atomic.LoadInt64(&ti.generationTime)
}

// IncreaseGenerationTime adds dur to the accumulated generationTime
func (ti *Index) IncreaseGenerationTime(dur int64) {
	_ = atomic.AddInt64(&ti.generationTime, dur)
}
