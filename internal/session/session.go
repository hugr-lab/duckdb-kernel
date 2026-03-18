package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
	"github.com/hugr-lab/duckdb-kernel/internal/spool"
	"github.com/hugr-lab/duckdb-kernel/pkg/flatten"
	"github.com/hugr-lab/duckdb-kernel/pkg/geoarrow"
)

// Session represents a DuckDB execution context.
type Session struct {
	ID             string
	Engine         *engine.Engine
	PreviewLimit   int
	executionCount int
	mu             sync.Mutex
	connector      *duckdb.Connector
}

// NewSession creates a new session with the given connector.
func NewSession(id string, connector *duckdb.Connector) *Session {
	return &Session{
		ID:           id,
		Engine:       engine.NewEngine(connector),
		PreviewLimit: 100,
		connector:    connector,
	}
}

// NextExecutionCount atomically increments and returns the new execution counter.
// Must be called once per execute request, before any messages are sent.
func (s *Session) NextExecutionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executionCount++
	return s.executionCount
}

// Execute runs a SQL query, streaming Arrow batches through a pipeline:
//
//	DuckDB RecordReader → Flatten → GeoArrow → Preview + Spool Write
//
// Flatten expands nested Struct/List/Map/Union to top-level columns.
// GeoArrow converts WKB binary geometry columns to native GeoArrow format.
// Records are NOT accumulated in memory.
func (s *Session) Execute(ctx context.Context, query string, sw *spool.StreamWriter) (*engine.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reader, err := s.Engine.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	// Pipeline: DuckDB → Flatten → GeoArrow
	flatReader := flatten.NewConverter(reader)
	defer flatReader.Release()

	geoReader := geoarrow.NewConverter(flatReader, geoarrow.WithBufferSize(2))
	defer geoReader.Release()

	result := &engine.QueryResult{}
	schemaSet := false

	for geoReader.Next() {
		rec := geoReader.RecordBatch()

		if !schemaSet {
			result.InitFromSchema(rec.Schema())
			schemaSet = true
		}

		result.AddPreviewRows(rec, s.PreviewLimit)

		if sw != nil {
			if err := sw.Write(rec); err != nil {
				return nil, fmt.Errorf("spool write: %w", err)
			}
		}
	}

	if err := geoReader.Err(); err != nil {
		return nil, fmt.Errorf("reading records: %w", err)
	}

	return result, nil
}

// ExecutionCount returns the current execution counter value.
func (s *Session) ExecutionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.executionCount
}

// SetPreviewLimit updates the preview row limit.
func (s *Session) SetPreviewLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PreviewLimit = limit
}

// Close closes the session's engine.
func (s *Session) Close() error {
	return s.Engine.Close()
}
