package engine

import (
	"encoding/json"
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

// GeometryColumnInfo holds metadata about a geometry column detected
// from Arrow schema field metadata or schema-level metadata.
type GeometryColumnInfo struct {
	Name   string `json:"name"`
	SRID   int    `json:"srid"`
	Format string `json:"format"` // "WKB", "GeoJSON", "H3Cell"
}

// QueryResult holds the output of a SQL execution.
type QueryResult struct {
	QueryID         string
	Columns         []ColumnMeta
	GeometryColumns []GeometryColumnInfo
	Rows            [][]string
	TotalRows       int64
	NumChunks       int
}

// AddPreviewRows adds rows from a record batch to the result preview.
// Returns true when the preview limit has been reached.
func (r *QueryResult) AddPreviewRows(rec arrow.RecordBatch, previewLimit int) bool {
	r.TotalRows += rec.NumRows()
	r.NumChunks++

	if len(r.Rows) >= previewLimit {
		return true
	}

	schema := rec.Schema()
	for rowIdx := int64(0); rowIdx < rec.NumRows(); rowIdx++ {
		if len(r.Rows) >= previewLimit {
			return true
		}
		row := make([]string, rec.NumCols())
		for colIdx := int64(0); colIdx < rec.NumCols(); colIdx++ {
			row[colIdx] = formatValue(rec.Column(int(colIdx)), int(rowIdx), schema.Field(int(colIdx)))
		}
		r.Rows = append(r.Rows, row)
	}
	return len(r.Rows) >= previewLimit
}

// InitFromSchema initializes column metadata and detects geometry columns.
func (r *QueryResult) InitFromSchema(schema *arrow.Schema) {
	r.Columns = make([]ColumnMeta, schema.NumFields())
	for i, field := range schema.Fields() {
		r.Columns[i] = ColumnMeta{
			Name: field.Name,
			Type: field.Type.String(),
		}
	}
	r.GeometryColumns = detectGeometryColumns(schema)
}

// detectGeometryColumns scans the Arrow schema for geometry columns using two
// complementary mechanisms:
// 1. Arrow field metadata: ARROW:extension:name = "geoarrow.wkb" or "ogc.wkb"
// 2. Schema metadata: X-Hugr-Geometry-Fields JSON header from HugrIPC
func detectGeometryColumns(schema *arrow.Schema) []GeometryColumnInfo {
	var geoCols []GeometryColumnInfo
	seen := make(map[string]bool)

	// Source 1: Arrow field metadata
	for _, field := range schema.Fields() {
		extName := ""
		if field.Metadata.Len() > 0 {
			idx := field.Metadata.FindKey("ARROW:extension:name")
			if idx >= 0 {
				extName = field.Metadata.Values()[idx]
			}
		}
		// Detect both WKB and native GeoArrow types
		format := ""
		switch extName {
		case "geoarrow.wkb", "ogc.wkb":
			format = "WKB"
		case "geoarrow.point", "geoarrow.multipoint":
			format = "GeoArrow:point"
		case "geoarrow.linestring", "geoarrow.multilinestring":
			format = "GeoArrow:linestring"
		case "geoarrow.polygon", "geoarrow.multipolygon":
			format = "GeoArrow:polygon"
		}
		if format != "" {
			info := GeometryColumnInfo{
				Name:   field.Name,
				SRID:   4326,
				Format: format,
			}
			// Try to parse SRID from extension metadata
			if field.Metadata.Len() > 0 {
				idx := field.Metadata.FindKey("ARROW:extension:metadata")
				if idx >= 0 {
					var extMeta struct {
						SRID int `json:"srid"`
					}
					if err := json.Unmarshal([]byte(field.Metadata.Values()[idx]), &extMeta); err == nil && extMeta.SRID > 0 {
						info.SRID = extMeta.SRID
					}
				}
			}
			geoCols = append(geoCols, info)
			seen[field.Name] = true
		}
	}

	// Source 2: Schema-level X-Hugr-Geometry-Fields metadata
	if schema.Metadata().Len() > 0 {
		idx := schema.Metadata().FindKey("X-Hugr-Geometry-Fields")
		if idx >= 0 {
			var fields []struct {
				Name   string `json:"name"`
				SRID   int    `json:"srid"`
				Format string `json:"format"`
			}
			if err := json.Unmarshal([]byte(schema.Metadata().Values()[idx]), &fields); err == nil {
				for _, f := range fields {
					if seen[f.Name] {
						continue
					}
					info := GeometryColumnInfo{
						Name:   f.Name,
						SRID:   f.SRID,
						Format: f.Format,
					}
					if info.SRID == 0 {
						info.SRID = 4326
					}
					if info.Format == "" {
						info.Format = "WKB"
					}
					geoCols = append(geoCols, info)
				}
			}
		}
	}

	return geoCols
}

// formatValue converts an Arrow array value at the given index to a string.
// GeoArrow geometry columns (detected by ARROW:extension:name metadata) are
// formatted as "{geometry}" instead of attempting to serialize nested structures.
func formatValue(col arrow.Array, idx int, field arrow.Field) string {
	if col.IsNull(idx) {
		return "NULL"
	}

	// Check for geometry extension types
	if field.Metadata.Len() > 0 {
		if mi := field.Metadata.FindKey("ARROW:extension:name"); mi >= 0 {
			ext := field.Metadata.Values()[mi]
			if strings.HasPrefix(ext, "geoarrow.") || ext == "ogc.wkb" {
				return "{geometry}"
			}
		}
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
