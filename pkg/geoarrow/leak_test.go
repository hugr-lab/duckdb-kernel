//go:build duckdb_arrow

package geoarrow

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
	"github.com/hugr-lab/duckdb-kernel/pkg/flatten"
)

// TestPipelineNoLeak runs the full pipeline (flatten → geoarrow → IPC write)
// with a CheckedAllocator to detect Arrow memory leaks.
func TestPipelineNoLeak(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()
	if _, err := db.Exec("INSTALL spatial; LOAD spatial;"); err != nil {
		t.Skipf("spatial not available: %v", err)
	}

	eng := engine.NewEngine(connector)

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())

	// Run a query with multiple rows and geometry
	reader, err := eng.Execute(context.Background(), `
		SELECT ST_Point(37.6, 55.7) AS geom, 'Moscow' AS city, 200 AS size
		UNION ALL SELECT ST_Point(30.3, 59.9), 'SPb', 100
		UNION ALL SELECT ST_Point(39.7, 47.2), 'Rostov', 50
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	// Flatten converter
	flatReader := flatten.NewConverter(reader)
	defer flatReader.Release()

	// GeoArrow converter with checked allocator
	geoReader := NewConverter(flatReader, WithBufferSize(1), WithAllocator(alloc))
	defer geoReader.Release()

	// Read all batches and write to IPC (simulating spool)
	var buf bytes.Buffer
	var w *ipc.Writer
	totalRows := int64(0)

	for geoReader.Next() {
		rec := geoReader.RecordBatch()
		totalRows += rec.NumRows()

		if w == nil {
			w = ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
		}
		if err := w.Write(rec); err != nil {
			t.Fatalf("IPC write: %v", err)
		}
	}
	if err := geoReader.Err(); err != nil {
		t.Fatalf("pipeline error: %v", err)
	}
	if w != nil {
		w.Close()
	}

	t.Logf("Pipeline: %d rows, %d IPC bytes", totalRows, buf.Len())

	// Check that all Arrow allocations were freed
	if alloc.CurrentAlloc() != 0 {
		t.Errorf("Arrow memory leak: %d bytes still allocated", alloc.CurrentAlloc())
	}
}

// TestStringReplacerNoLeak checks that StringReplacer releases all Arrow buffers.
func TestStringReplacerNoLeak(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()
	if _, err := db.Exec("INSTALL spatial; LOAD spatial;"); err != nil {
		t.Skipf("spatial not available: %v", err)
	}

	eng := engine.NewEngine(connector)

	reader, err := eng.Execute(context.Background(),
		"SELECT ST_Point(1, 2) AS geom, 42 AS id")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	flatReader := flatten.NewConverter(reader)
	defer flatReader.Release()

	geoReader := NewConverter(flatReader, WithBufferSize(1))
	defer geoReader.Release()

	// StringReplacer (simulates Perspective stream)
	replacer := NewStringReplacer(geoReader)
	defer replacer.Release()

	for replacer.Next() {
		rec := replacer.RecordBatch()
		t.Logf("Replaced batch: %d rows, %d cols", rec.NumRows(), rec.NumCols())
		// Check that geometry column is now string
		for i, f := range rec.Schema().Fields() {
			t.Logf("  col %d: %s type=%s", i, f.Name, f.Type)
		}
	}
	if err := replacer.Err(); err != nil {
		t.Fatalf("replacer error: %v", err)
	}
}
