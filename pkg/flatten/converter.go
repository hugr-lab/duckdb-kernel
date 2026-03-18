// Package flatten provides a RecordReader wrapper that flattens
// complex Arrow types (Struct, List, Map, Union) to simple columns.
package flatten

import (
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	qetypes "github.com/hugr-lab/query-engine/types"
)

// Converter wraps an array.RecordReader and flattens complex types.
// Implements array.RecordReader.
type Converter struct {
	refs    atomic.Int64
	source  array.RecordReader
	schema  *arrow.Schema
	current arrow.RecordBatch
	err     error
	flatten bool
	checked bool
	mem     memory.Allocator
}

// NewConverter creates a flattening RecordReader wrapper.
// If the schema has no complex types, batches pass through unchanged.
func NewConverter(source array.RecordReader) *Converter {
	c := &Converter{
		source: source,
		schema: source.Schema(),
		mem:    memory.DefaultAllocator,
	}
	c.refs.Store(1)
	return c
}

func (c *Converter) Retain()  { c.refs.Add(1) }
func (c *Converter) Release() {
	if c.refs.Add(-1) == 0 {
		if c.current != nil {
			c.current.Release()
			c.current = nil
		}
	}
}

func (c *Converter) Schema() *arrow.Schema { return c.schema }

func (c *Converter) Next() bool {
	if c.current != nil {
		c.current.Release()
		c.current = nil
	}

	if !c.source.Next() {
		c.err = c.source.Err()
		return false
	}

	rec := c.source.RecordBatch()

	if !c.checked {
		c.flatten = qetypes.NeedsFlatten(rec.Schema())
		c.checked = true
	}

	if c.flatten {
		flat := qetypes.FlattenRecord(rec, c.mem)
		c.current = flat
		c.schema = flat.Schema()
	} else {
		rec.Retain()
		c.current = rec
	}

	return true
}

func (c *Converter) RecordBatch() arrow.RecordBatch { return c.current }
func (c *Converter) Record() arrow.RecordBatch      { return c.current }
func (c *Converter) Err() error                      { return c.err }
