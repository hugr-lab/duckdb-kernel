package session

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
	"github.com/hugr-lab/duckdb-kernel/internal/spool"
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

// Execute runs a SQL query, streaming Arrow batches to the writer.
// Records are NOT accumulated in memory.
func (s *Session) Execute(ctx context.Context, query string, sw *spool.StreamWriter) (*engine.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reader, err := s.Engine.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	var onBatch func(rec arrow.Record) error
	if sw != nil {
		onBatch = sw.Write
	}

	return engine.BuildPreviewFromReader(reader, s.PreviewLimit, onBatch)
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
