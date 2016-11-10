package bloom

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

// Generation
func (ti *Index) Generation() uint64 {
	return atomic.LoadUint64(&ti.generation)
}

func (ti *Index) IncrementGeneration() {
	newGen := uint64(ti.Generation() + 1)
	_ = atomic.SwapUint64(&ti.generation, newGen)
}
