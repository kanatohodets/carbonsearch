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

// Generation
func (fi *Index) Generation() uint64 {
	return atomic.LoadUint64(&fi.generation)
}

func (fi *Index) IncrementGeneration() {
	newGen := uint64(fi.Generation() + 1)
	_ = atomic.SwapUint64(&fi.generation, newGen)
}
