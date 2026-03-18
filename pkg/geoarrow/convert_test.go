//go:build duckdb_arrow

package geoarrow

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
)

func setupEngine(t *testing.T) *engine.Engine {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connector.Close() })

	db := sql.OpenDB(connector)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("INSTALL spatial; LOAD spatial;"); err != nil {
		t.Skipf("spatial not available: %v", err)
	}

	return engine.NewEngine(connector)
}

func TestConvertBatch_Point(t *testing.T) {
	eng := setupEngine(t)
	reader, err := eng.Execute(context.Background(),
		"SELECT ST_Point(37.6173, 55.7558) AS geom, 'Moscow' AS city, 200 AS size")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("no batches")
	}
	rec := reader.RecordBatch()

	t.Logf("schema: %s", rec.Schema())
	geoCols := DetectGeometryColumns(rec.Schema())
	if len(geoCols) == 0 {
		t.Fatal("no geometry columns detected")
	}
	t.Logf("geoCols: %+v", geoCols)

	converted, gt, err := ConvertBatch(rec, geoCols[0], GeoTypeUnknown, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ConvertBatch: %v", err)
	}
	defer converted.Release()

	t.Logf("geoType: %d", gt)
	t.Logf("converted schema: %s", converted.Schema())

	// Write to IPC — verifies structure is valid
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(converted.Schema()))
	if err := w.Write(converted); err != nil {
		t.Fatalf("IPC write: %v", err)
	}
	w.Close()
	t.Logf("IPC write OK, %d bytes", buf.Len())
}

func TestConvertBatch_Polygon(t *testing.T) {
	eng := setupEngine(t)
	reader, err := eng.Execute(context.Background(),
		"SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))') AS geom, 'test' AS name")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("no batches")
	}
	rec := reader.RecordBatch()

	geoCols := DetectGeometryColumns(rec.Schema())
	if len(geoCols) == 0 {
		t.Fatal("no geometry columns")
	}

	converted, gt, err := ConvertBatch(rec, geoCols[0], GeoTypeUnknown, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ConvertBatch: %v", err)
	}
	defer converted.Release()

	t.Logf("geoType: %d, schema: %s", gt, converted.Schema())

	// Debug: inspect geometry column structure
	geomCol := converted.Column(0)
	t.Logf("geomCol type=%s len=%d nulls=%d", geomCol.DataType(), geomCol.Len(), geomCol.NullN())
	d := geomCol.Data()
	t.Logf("data: len=%d offset=%d buffers=%d children=%d", d.Len(), d.Offset(), len(d.Buffers()), len(d.Children()))
	for bi, b := range d.Buffers() {
		if b != nil {
			t.Logf("  buf[%d]: len=%d", bi, b.Len())
		} else {
			t.Logf("  buf[%d]: nil", bi)
		}
	}
	var dumpData func(prefix string, ad arrow.ArrayData)
	dumpData = func(prefix string, ad arrow.ArrayData) {
		t.Logf("%stype=%T len=%d offset=%d bufs=%d children=%d",
			prefix, ad.DataType(), ad.Len(), ad.Offset(), len(ad.Buffers()), len(ad.Children()))
		for bi, b := range ad.Buffers() {
			if b != nil {
				t.Logf("%s  buf[%d]: len=%d", prefix, bi, b.Len())
			}
		}
		for ci, ch := range ad.Children() {
			t.Logf("%s  child[%d]:", prefix, ci)
			dumpData(prefix+"    ", ch)
		}
	}
	dumpData("", d)

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(converted.Schema()))
	if err := w.Write(converted); err != nil {
		t.Fatalf("IPC write: %v", err)
	}
	w.Close()
	t.Logf("IPC write OK, %d bytes", buf.Len())
}

func TestConvertBatch_LineString(t *testing.T) {
	eng := setupEngine(t)
	reader, err := eng.Execute(context.Background(),
		"SELECT ST_GeomFromText('LINESTRING(0 0, 1 1, 2 0)') AS geom, 'route' AS name")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("no batches")
	}
	rec := reader.RecordBatch()
	geoCols := DetectGeometryColumns(rec.Schema())
	if len(geoCols) == 0 {
		t.Fatal("no geometry columns")
	}

	converted, gt, err := ConvertBatch(rec, geoCols[0], GeoTypeUnknown, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ConvertBatch: %v", err)
	}
	defer converted.Release()
	t.Logf("geoType: %d, schema: %s", gt, converted.Schema())

	if gt != GeoTypeLine {
		t.Fatalf("expected GeoTypeLine(%d), got %d", GeoTypeLine, gt)
	}

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(converted.Schema()))
	if err := w.Write(converted); err != nil {
		t.Fatalf("IPC write: %v", err)
	}
	w.Close()
	t.Logf("IPC write OK, %d bytes", buf.Len())
}

func TestConvertBatch_MultiLineString(t *testing.T) {
	eng := setupEngine(t)
	reader, err := eng.Execute(context.Background(),
		"SELECT ST_GeomFromText('MULTILINESTRING((0 0,1 1),(2 2,3 3,4 4))') AS geom")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("no batches")
	}
	rec := reader.RecordBatch()
	geoCols := DetectGeometryColumns(rec.Schema())
	if len(geoCols) == 0 {
		t.Fatal("no geometry columns")
	}

	converted, gt, err := ConvertBatch(rec, geoCols[0], GeoTypeUnknown, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ConvertBatch: %v", err)
	}
	defer converted.Release()
	t.Logf("geoType: %d, schema: %s", gt, converted.Schema())

	if gt != GeoTypeLine {
		t.Fatalf("expected GeoTypeLine(%d), got %d", GeoTypeLine, gt)
	}

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(converted.Schema()))
	if err := w.Write(converted); err != nil {
		t.Fatalf("IPC write: %v", err)
	}
	w.Close()
	t.Logf("IPC write OK, %d bytes", buf.Len())
}

func TestConvertBatch_MultiPolygon(t *testing.T) {
	eng := setupEngine(t)
	reader, err := eng.Execute(context.Background(),
		"SELECT ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))') AS geom")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("no batches")
	}
	rec := reader.RecordBatch()
	geoCols := DetectGeometryColumns(rec.Schema())
	if len(geoCols) == 0 {
		t.Fatal("no geometry columns")
	}

	converted, gt, err := ConvertBatch(rec, geoCols[0], GeoTypeUnknown, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ConvertBatch: %v", err)
	}
	defer converted.Release()
	t.Logf("geoType: %d, schema: %s", gt, converted.Schema())

	if gt != GeoTypePolygon {
		t.Fatalf("expected GeoTypePolygon(%d), got %d", GeoTypePolygon, gt)
	}

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(converted.Schema()))
	if err := w.Write(converted); err != nil {
		t.Fatalf("IPC write: %v", err)
	}
	w.Close()
	t.Logf("IPC write OK, %d bytes", buf.Len())
}

func TestConvertBatch_MultiplePoints(t *testing.T) {
	eng := setupEngine(t)
	reader, err := eng.Execute(context.Background(), `
		SELECT ST_Point(37.6173, 55.7558) AS geom, 'Moscow' AS city
		UNION ALL
		SELECT ST_Point(30.3351, 59.9343), 'St. Petersburg'
		UNION ALL
		SELECT ST_Point(39.7006, 47.2357), 'Rostov'
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	// Use Converter as a RecordReader to handle multiple batches
	converter := NewConverter(reader, WithBufferSize(1))
	defer converter.Release()

	totalRows := int64(0)
	var buf bytes.Buffer
	var w *ipc.Writer
	for converter.Next() {
		rec := converter.RecordBatch()
		totalRows += rec.NumRows()
		if w == nil {
			w = ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
		}
		if err := w.Write(rec); err != nil {
			t.Fatalf("IPC write: %v", err)
		}
		t.Logf("batch: %d rows, schema: %s", rec.NumRows(), rec.Schema())
	}
	if err := converter.Err(); err != nil {
		t.Fatalf("converter error: %v", err)
	}
	if w != nil {
		w.Close()
	}

	if totalRows != 3 {
		t.Fatalf("expected 3 total rows, got %d", totalRows)
	}
	t.Logf("IPC write OK, %d bytes, %d total rows", buf.Len(), totalRows)
}
