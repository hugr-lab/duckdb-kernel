package session

import (
	"context"
	"sync"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
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
		PreviewLimit: 50,
		connector:    connector,
	}
}

// Execute runs a SQL query within the session, serializing access.
// Returns a QueryResult with preview rows and Arrow records.
func (s *Session) Execute(ctx context.Context, query string) (*engine.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.executionCount++

	reader, err := s.Engine.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	result, err := engine.BuildFromReader(reader, s.PreviewLimit)
	if err != nil {
		return nil, err
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
