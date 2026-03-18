package geoarrow

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// GeoArrow extension type names.
const (
	ExtMultiPoint       = "geoarrow.multipoint"
	ExtMultiLineString  = "geoarrow.multilinestring"
	ExtMultiPolygon     = "geoarrow.multipolygon"
)

// CoordType is FixedSizeList(2, Float64) — interleaved XY coordinates.
var CoordType = arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64)

// GeoArrow nested types — always Multi* for schema consistency.
var (
	// MultiPoint: List<FixedSizeList<Float64>[2]>
	MultiPointType = arrow.ListOf(CoordType)

	// MultiLineString: List<List<FixedSizeList<Float64>[2]>>
	MultiLineStringType = arrow.ListOf(arrow.ListOf(CoordType))

	// MultiPolygon: List<List<List<FixedSizeList<Float64>[2]>>>
	MultiPolygonType = arrow.ListOf(arrow.ListOf(arrow.ListOf(CoordType)))
)

// ArrowTypeForGeo returns the Arrow DataType and extension name for a GeoType.
func ArrowTypeForGeo(gt GeoType) (arrow.DataType, string) {
	switch gt {
	case GeoTypePoint:
		return MultiPointType, ExtMultiPoint
	case GeoTypeLine:
		return MultiLineStringType, ExtMultiLineString
	case GeoTypePolygon:
		return MultiPolygonType, ExtMultiPolygon
	default:
		return nil, ""
	}
}

// GeoArrowField creates a new Arrow Field with GeoArrow extension metadata.
// extensionMeta is the original ARROW:extension:metadata value (passed through as-is).
// If empty, defaults to {"srid":4326}.
func GeoArrowField(name string, gt GeoType, extensionMeta string) (arrow.Field, error) {
	dt, ext := ArrowTypeForGeo(gt)
	if dt == nil {
		return arrow.Field{}, fmt.Errorf("unsupported geometry type: %d", gt)
	}
	if extensionMeta == "" {
		extensionMeta = `{"srid":4326}`
	}
	meta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{ext, extensionMeta},
	)
	return arrow.Field{
		Name:     name,
		Type:     dt,
		Nullable: true,
		Metadata: meta,
	}, nil
}

// GeometryColumn describes a geometry column to convert.
type GeometryColumn struct {
	Name          string
	Index         int
	SRID          int
	Format        string // "WKB", "GeoJSON", "H3Cell"
	ExtensionMeta string // original ARROW:extension:metadata (passed through)
}

// DetectGeometryColumns finds geometry columns in an Arrow schema
// by checking ARROW:extension:name metadata.
func DetectGeometryColumns(schema *arrow.Schema) []GeometryColumn {
	var cols []GeometryColumn
	for i, f := range schema.Fields() {
		if f.Metadata.Len() == 0 {
			continue
		}
		idx := f.Metadata.FindKey("ARROW:extension:name")
		if idx < 0 {
			continue
		}
		ext := f.Metadata.Values()[idx]
		// Read original extension metadata (contains srid etc.)
		extMeta := ""
		if mi := f.Metadata.FindKey("ARROW:extension:metadata"); mi >= 0 {
			extMeta = f.Metadata.Values()[mi]
		}
		srid := parseSRID(extMeta)

		switch ext {
		case "geoarrow.wkb", "ogc.wkb":
			// WKB binary — needs conversion
			cols = append(cols, GeometryColumn{
				Name:          f.Name,
				Index:         i,
				SRID:          srid,
				Format:        "WKB",
				ExtensionMeta: extMeta,
			})

		case ExtMultiPoint, ExtMultiLineString, ExtMultiPolygon,
			"geoarrow.point", "geoarrow.linestring", "geoarrow.polygon":
			// Already native GeoArrow — skip conversion
			continue
		}
	}
	return cols
}

// parseSRID extracts srid from ARROW:extension:metadata JSON.
// Returns 4326 if not found or not parseable.
func parseSRID(extMeta string) int {
	if extMeta == "" {
		return 4326
	}
	var m struct {
		SRID int `json:"srid"`
	}
	if err := json.Unmarshal([]byte(extMeta), &m); err != nil || m.SRID == 0 {
		return 4326
	}
	return m.SRID
}
