package engine

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ColumnMeta holds metadata for a single result column.
type ColumnMeta struct {
	Name string
	Type string
}

// QueryResult holds the output of a SQL execution.
type QueryResult struct {
	QueryID   string
	Columns   []ColumnMeta
	Rows      [][]string
	TotalRows int64
	Records   []arrow.Record
}

// Release releases all Arrow records held by the result.
func (r *QueryResult) Release() {
	for _, rec := range r.Records {
		rec.Release()
	}
	r.Records = nil
}

// BuildFromReader reads all record batches from a reader and builds
// the preview rows up to the given limit.
func BuildFromReader(reader array.RecordReader, limit int) (*QueryResult, error) {
	if reader == nil {
		return &QueryResult{}, nil
	}

	result := &QueryResult{}
	schema := reader.Schema()

	// Extract column metadata
	result.Columns = make([]ColumnMeta, schema.NumFields())
	for i, field := range schema.Fields() {
		result.Columns[i] = ColumnMeta{
			Name: field.Name,
			Type: field.Type.String(),
		}
	}

	// Read all record batches
	previewFull := false
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		result.Records = append(result.Records, rec)
		result.TotalRows += rec.NumRows()

		// Build preview rows
		if !previewFull {
			for rowIdx := int64(0); rowIdx < rec.NumRows(); rowIdx++ {
				if len(result.Rows) >= limit {
					previewFull = true
					break
				}
				row := make([]string, rec.NumCols())
				for colIdx := int64(0); colIdx < rec.NumCols(); colIdx++ {
					row[colIdx] = formatValue(rec.Column(int(colIdx)), int(rowIdx))
				}
				result.Rows = append(result.Rows, row)
			}
		}
	}

	if err := reader.Err(); err != nil {
		result.Release()
		return nil, fmt.Errorf("reading records: %w", err)
	}

	return result, nil
}

// formatValue converts an Arrow array value at the given index to a string.
func formatValue(col arrow.Array, idx int) string {
	if col.IsNull(idx) {
		return "NULL"
	}

	switch c := col.(type) {
	case *array.Int8:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Int16:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Int32:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Int64:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Uint8:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Uint16:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Uint32:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Uint64:
		return fmt.Sprintf("%d", c.Value(idx))
	case *array.Float32:
		return fmt.Sprintf("%g", c.Value(idx))
	case *array.Float64:
		return fmt.Sprintf("%g", c.Value(idx))
	case *array.String:
		return c.Value(idx)
	case *array.LargeString:
		return c.Value(idx)
	case *array.Boolean:
		return fmt.Sprintf("%t", c.Value(idx))
	case *array.Date32:
		return c.Value(idx).FormattedString()
	case *array.Date64:
		return c.Value(idx).FormattedString()
	case *array.Timestamp:
		return fmt.Sprintf("%v", c.Value(idx).ToTime(c.DataType().(*arrow.TimestampType).Unit))
	case *array.Binary:
		return fmt.Sprintf("%x", c.Value(idx))
	default:
		return fmt.Sprintf("%v", c.ValueStr(idx))
	}
}
