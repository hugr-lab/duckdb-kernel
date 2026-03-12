package engine

import (
	"fmt"
	"strings"

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
	NumChunks int
}

// BuildPreviewFromReader reads record batches, builds preview rows,
// and streams each batch to the provided callback without accumulating
// all records in memory.
func BuildPreviewFromReader(
	reader array.RecordReader,
	previewLimit int,
	onBatch func(rec arrow.Record) error,
) (*QueryResult, error) {
	if reader == nil {
		return &QueryResult{}, nil
	}

	result := &QueryResult{}
	schema := reader.Schema()

	// Extract column metadata.
	result.Columns = make([]ColumnMeta, schema.NumFields())
	for i, field := range schema.Fields() {
		result.Columns[i] = ColumnMeta{
			Name: field.Name,
			Type: field.Type.String(),
		}
	}

	previewFull := false
	for reader.Next() {
		rec := reader.Record()
		result.TotalRows += rec.NumRows()

		// Build preview rows from first batches.
		if !previewFull {
			for rowIdx := int64(0); rowIdx < rec.NumRows(); rowIdx++ {
				if len(result.Rows) >= previewLimit {
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

		// Stream batch to writer (spool).
		if onBatch != nil {
			if err := onBatch(rec); err != nil {
				return nil, fmt.Errorf("batch callback: %w", err)
			}
		}
	}

	if err := reader.Err(); err != nil {
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
		return strings.Clone(c.Value(idx))
	case *array.LargeString:
		return strings.Clone(c.Value(idx))
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
