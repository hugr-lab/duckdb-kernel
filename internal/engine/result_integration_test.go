//go:build duckdb_arrow

package engine

import (
	"context"
	"database/sql"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// TestGeometryDetection_DuckDBSpatial tests end-to-end geometry column detection
// by executing a spatial query through DuckDB and checking the resulting metadata.
func TestGeometryDetection_DuckDBSpatial(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatalf("failed to create connector: %v", err)
	}
	defer connector.Close()

	// Install and load spatial extension
	db := sql.OpenDB(connector)
	defer db.Close()

	_, err = db.Exec("INSTALL spatial; LOAD spatial;")
	if err != nil {
		t.Skipf("spatial extension not available: %v", err)
	}

	engine := NewEngine(connector)
	ctx := context.Background()

	// Helper: read all batches and build preview
	buildResult := func(t *testing.T, query string) *QueryResult {
		t.Helper()
		reader, err := engine.Execute(ctx, query)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		defer reader.Release()

		result := &QueryResult{}
		schemaSet := false
		for reader.Next() {
			rec := reader.RecordBatch()
			if !schemaSet {
				result.InitFromSchema(rec.Schema())
				schemaSet = true
			}
			result.AddPreviewRows(rec, 100)
		}
		if err := reader.Err(); err != nil {
			t.Fatalf("reading records: %v", err)
		}
		return result
	}

	// Test 1: ST_Point generates geoarrow.wkb extension type
	t.Run("ST_Point", func(t *testing.T) {
		result := buildResult(t, "SELECT ST_Point(1.0, 2.0) AS geom, 42 AS id")

		if len(result.GeometryColumns) == 0 {
			t.Fatal("expected geometry columns to be detected, got 0")
		}

		gc := result.GeometryColumns[0]
		t.Logf("detected geometry column: %+v", gc)

		if gc.Name != "geom" {
			t.Errorf("expected column name 'geom', got %q", gc.Name)
		}
		if gc.Format != "WKB" {
			t.Errorf("expected format 'WKB', got %q", gc.Format)
		}
	})

	// Test 2: ST_GeomFromText generates geoarrow.wkb
	t.Run("ST_GeomFromText", func(t *testing.T) {
		result := buildResult(t, "SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))') AS polygon_geom")

		if len(result.GeometryColumns) == 0 {
			t.Fatal("expected geometry columns to be detected, got 0")
		}

		gc := result.GeometryColumns[0]
		t.Logf("detected geometry column: %+v", gc)

		if gc.Name != "polygon_geom" {
			t.Errorf("expected column name 'polygon_geom', got %q", gc.Name)
		}
	})

	// Test 3: Non-geometry query should have no geometry columns
	t.Run("NoGeometry", func(t *testing.T) {
		result := buildResult(t, "SELECT 1 AS id, 'hello' AS name")

		if len(result.GeometryColumns) != 0 {
			t.Errorf("expected 0 geometry columns, got %d: %+v", len(result.GeometryColumns), result.GeometryColumns)
		}
	})

	// Test 4: Multiple geometry columns
	t.Run("MultipleGeomColumns", func(t *testing.T) {
		result := buildResult(t,
			"SELECT ST_Point(1, 2) AS point1, ST_Point(3, 4) AS point2, 'test' AS label")

		if len(result.GeometryColumns) < 2 {
			t.Fatalf("expected at least 2 geometry columns, got %d", len(result.GeometryColumns))
		}

		names := make(map[string]bool)
		for _, gc := range result.GeometryColumns {
			names[gc.Name] = true
			t.Logf("detected: %+v", gc)
		}

		if !names["point1"] {
			t.Error("expected 'point1' in geometry columns")
		}
		if !names["point2"] {
			t.Error("expected 'point2' in geometry columns")
		}
	})
}
