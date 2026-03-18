package geoarrow

import (
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StringReplacer wraps a RecordReader and replaces geometry columns
// (WKB binary or native GeoArrow) with a Utf8 string column containing
// "{geometry}" for display in viewers that don't support binary/nested types.
//
// Implements array.RecordReader.
type StringReplacer struct {
	refs    atomic.Int64
	source  array.RecordReader
	schema  *arrow.Schema
	geoCols []int // column indices to replace
	current arrow.RecordBatch
	err     error
	mem     memory.Allocator
}

// NewStringReplacer creates a RecordReader that replaces geometry columns with "{geometry}".
// It auto-detects geometry columns by ARROW:extension:name metadata
// (geoarrow.wkb, ogc.wkb, geoarrow.point, geoarrow.multi*, etc.).
func NewStringReplacer(source array.RecordReader) *StringReplacer {
	geoCols := findGeomColumnIndices(source.Schema())
	r := &StringReplacer{
		source:  source,
		schema:  source.Schema(),
		geoCols: geoCols,
		mem:     memory.DefaultAllocator,
	}
	r.refs.Store(1)
	return r
}

func findGeomColumnIndices(schema *arrow.Schema) []int {
	var indices []int
	for i, f := range schema.Fields() {
		if f.Metadata.Len() == 0 {
			continue
		}
		idx := f.Metadata.FindKey("ARROW:extension:name")
		if idx < 0 {
			continue
		}
		ext := f.Metadata.Values()[idx]
		if strings.HasPrefix(ext, "geoarrow.") || ext == "ogc.wkb" {
			indices = append(indices, i)
		}
	}
	return indices
}

func (r *StringReplacer) Retain()  { r.refs.Add(1) }
func (r *StringReplacer) Release() {
	if r.refs.Add(-1) == 0 {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
	}
}

func (r *StringReplacer) Schema() *arrow.Schema { return r.schema }

func (r *StringReplacer) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	if !r.source.Next() {
		r.err = r.source.Err()
		return false
	}

	rec := r.source.RecordBatch()

	if len(r.geoCols) == 0 {
		rec.Retain()
		r.current = rec
		return true
	}

	r.current = r.replaceColumns(rec)
	// Update schema from first replaced batch
	if r.current != nil && r.current.Schema() != r.schema {
		r.schema = r.current.Schema()
	}
	return true
}

func (r *StringReplacer) replaceColumns(rec arrow.RecordBatch) arrow.RecordBatch {
	numRows := int(rec.NumRows())
	result := rec

	for _, colIdx := range r.geoCols {
		if colIdx >= int(result.NumCols()) {
			continue
		}

		// Build string column with "{geometry}" for non-null rows
		bldr := array.NewStringBuilder(r.mem)
		origCol := result.Column(colIdx)
		for i := 0; i < numRows; i++ {
			if origCol.IsNull(i) {
				bldr.AppendNull()
			} else {
				bldr.Append("{geometry}")
			}
		}
		strArr := bldr.NewArray()
		bldr.Release()

		// Build new schema with string field
		field := result.Schema().Field(colIdx)
		newField := arrow.Field{
			Name:     field.Name,
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		}
		fields := make([]arrow.Field, len(result.Schema().Fields()))
		copy(fields, result.Schema().Fields())
		fields[colIdx] = newField
		meta := result.Schema().Metadata()
		newSchema := arrow.NewSchema(fields, &meta)

		// Build new column list
		cols := make([]arrow.Array, result.NumCols())
		for i := range int(result.NumCols()) {
			if i == colIdx {
				cols[i] = strArr
			} else {
				cols[i] = result.Column(i)
			}
		}

		newRec := array.NewRecordBatch(newSchema, cols, int64(numRows))
		strArr.Release()

		if result != rec {
			result.Release()
		}
		result = newRec
	}

	return result
}

func (r *StringReplacer) RecordBatch() arrow.RecordBatch { return r.current }
func (r *StringReplacer) Record() arrow.RecordBatch      { return r.current }
func (r *StringReplacer) Err() error                      { return r.err }
