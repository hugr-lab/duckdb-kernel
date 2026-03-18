package geoarrow

import (
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Projector wraps a RecordReader and projects (selects) only the requested columns.
// Implements array.RecordReader.
type Projector struct {
	refs    atomic.Int64
	source  array.RecordReader
	schema  *arrow.Schema
	indices []int // column indices to keep
	current arrow.RecordBatch
	err     error
}

// NewProjector creates a RecordReader that outputs only the named columns.
// Columns not found in the schema are silently ignored.
// If all columns are present or cols is empty, returns source unchanged (no wrapper).
func NewProjector(source array.RecordReader, cols map[string]bool) array.RecordReader {
	if len(cols) == 0 {
		return source
	}

	schema := source.Schema()
	var indices []int
	for i, f := range schema.Fields() {
		if cols[f.Name] {
			indices = append(indices, i)
		}
	}

	// No projection needed — all columns requested
	if len(indices) == len(schema.Fields()) || len(indices) == 0 {
		return source
	}

	// Build projected schema
	fields := make([]arrow.Field, len(indices))
	for i, idx := range indices {
		fields[i] = schema.Field(idx)
	}
	meta := schema.Metadata()
	newSchema := arrow.NewSchema(fields, &meta)

	p := &Projector{
		source:  source,
		schema:  newSchema,
		indices: indices,
	}
	p.refs.Store(1)
	return p
}

func (p *Projector) Retain()  { p.refs.Add(1) }
func (p *Projector) Release() {
	if p.refs.Add(-1) == 0 {
		if p.current != nil {
			p.current.Release()
			p.current = nil
		}
	}
}

func (p *Projector) Schema() *arrow.Schema { return p.schema }

func (p *Projector) Next() bool {
	if p.current != nil {
		p.current.Release()
		p.current = nil
	}

	if !p.source.Next() {
		p.err = p.source.Err()
		return false
	}

	rec := p.source.RecordBatch()

	// Project columns
	arrays := make([]arrow.Array, len(p.indices))
	for i, idx := range p.indices {
		arrays[i] = rec.Column(idx)
	}
	p.current = array.NewRecordBatch(p.schema, arrays, rec.NumRows())
	return true
}

func (p *Projector) RecordBatch() arrow.RecordBatch { return p.current }
func (p *Projector) Record() arrow.RecordBatch      { return p.current }
func (p *Projector) Err() error                      { return p.err }
