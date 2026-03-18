package engine

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestDetectGeometryColumns_ArrowExtension(t *testing.T) {
	// Simulate Arrow schema with geoarrow.wkb extension type metadata
	fieldMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"geoarrow.wkb", `{"srid":4326}`},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: fieldMeta},
	}, nil)

	cols := detectGeometryColumns(schema)
	if len(cols) != 1 {
		t.Fatalf("expected 1 geometry column, got %d", len(cols))
	}
	if cols[0].Name != "geom" {
		t.Errorf("expected column name 'geom', got %q", cols[0].Name)
	}
	if cols[0].Format != "WKB" {
		t.Errorf("expected format 'WKB', got %q", cols[0].Format)
	}
	if cols[0].SRID != 4326 {
		t.Errorf("expected SRID 4326, got %d", cols[0].SRID)
	}
}

func TestDetectGeometryColumns_OgcWkb(t *testing.T) {
	fieldMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name"},
		[]string{"ogc.wkb"},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geometry", Type: arrow.BinaryTypes.Binary, Metadata: fieldMeta},
	}, nil)

	cols := detectGeometryColumns(schema)
	if len(cols) != 1 {
		t.Fatalf("expected 1 geometry column, got %d", len(cols))
	}
	if cols[0].Name != "geometry" {
		t.Errorf("expected column name 'geometry', got %q", cols[0].Name)
	}
	if cols[0].SRID != 4326 {
		t.Errorf("expected default SRID 4326, got %d", cols[0].SRID)
	}
}

func TestDetectGeometryColumns_HugrMetadata(t *testing.T) {
	// Schema-level X-Hugr-Geometry-Fields metadata
	schemaMeta := arrow.NewMetadata(
		[]string{"X-Hugr-Geometry-Fields"},
		[]string{`[{"name":"location","srid":3857,"format":"WKB"},{"name":"h3_cell","srid":0,"format":"H3Cell"}]`},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "location", Type: arrow.BinaryTypes.Binary},
		{Name: "h3_cell", Type: arrow.BinaryTypes.String},
	}, &schemaMeta)

	cols := detectGeometryColumns(schema)
	if len(cols) != 2 {
		t.Fatalf("expected 2 geometry columns, got %d", len(cols))
	}
	if cols[0].Name != "location" || cols[0].SRID != 3857 || cols[0].Format != "WKB" {
		t.Errorf("cols[0] = %+v, expected location/3857/WKB", cols[0])
	}
	if cols[1].Name != "h3_cell" || cols[1].Format != "H3Cell" {
		t.Errorf("cols[1] = %+v, expected h3_cell/H3Cell", cols[1])
	}
}

func TestDetectGeometryColumns_Dedup(t *testing.T) {
	// Same column in both Arrow field metadata and Hugr metadata — should not duplicate
	fieldMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name"},
		[]string{"geoarrow.wkb"},
	)
	schemaMeta := arrow.NewMetadata(
		[]string{"X-Hugr-Geometry-Fields"},
		[]string{`[{"name":"geom","srid":4326,"format":"WKB"}]`},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: fieldMeta},
	}, &schemaMeta)

	cols := detectGeometryColumns(schema)
	if len(cols) != 1 {
		t.Fatalf("expected 1 geometry column (dedup), got %d", len(cols))
	}
}

func TestDetectGeometryColumns_NoGeometry(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	cols := detectGeometryColumns(schema)
	if len(cols) != 0 {
		t.Fatalf("expected 0 geometry columns, got %d", len(cols))
	}
}

func TestDetectGeometryColumns_CustomSRID(t *testing.T) {
	fieldMeta := arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"geoarrow.wkb", `{"srid":32632}`},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "geom", Type: arrow.BinaryTypes.Binary, Metadata: fieldMeta},
	}, nil)

	cols := detectGeometryColumns(schema)
	if len(cols) != 1 {
		t.Fatalf("expected 1 geometry column, got %d", len(cols))
	}
	if cols[0].SRID != 32632 {
		t.Errorf("expected SRID 32632, got %d", cols[0].SRID)
	}
}
