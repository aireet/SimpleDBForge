package utils

import (
	"bytes"
	"sync"
)

// Pool is the global buffer pool instance for reuse of bytes.Buffer objects.
// Using a pool reduces GC pressure for applications that frequently allocate
// and discard buffers.
var Pool = NewBufferPool()

// BufferPool manages a pool of reusable bytes.Buffer objects.
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a new BufferPool with the default factory function.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get retrieves a buffer from the pool, or creates a new one if the pool is empty.
func (p *BufferPool) Get() *bytes.Buffer {
	buf, ok := p.pool.Get().(*bytes.Buffer)
	if !ok {
		return new(bytes.Buffer)
	}
	return buf
}

// Put returns a buffer to the pool for reuse. The buffer is reset before pooling.
// Nil buffers are silently ignored.
func (p *BufferPool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	p.pool.Put(buf)
}
