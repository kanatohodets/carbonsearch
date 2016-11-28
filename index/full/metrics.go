package full

import (
	"sync/atomic"
)

// Tags
func (fi *Index) ReadableTags() uint32 {
	return atomic.LoadUint32(&fi.readableTags)
}

func (fi *Index) SetReadableTags(new uint32) {
	_ = atomic.SwapUint32(&fi.readableTags, new)
}

// Generation convenience method
func (fi *Index) Generation() uint64 {
	return atomic.LoadUint64(&fi.generation)
}

// IncrementGeneration incriments the generation number
func (fi *Index) IncrementGeneration() {
	_ = atomic.AddUint64(&fi.generation, 1)
}

// GenerationTime convenience method
func (fi *Index) GenerationTime() int64 {
	return atomic.LoadInt64(&fi.generationTime)
}

// IncreaseGenerationTime adds dur to the accumulated generationTime
func (fi *Index) IncreaseGenerationTime(dur int64) {
	_ = atomic.AddInt64(&fi.generationTime, dur)
}
