package engine

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/duckdb/duckdb-go/v2"
)

// Engine executes SQL queries via DuckDB and returns Arrow results.
type Engine struct {
	connector *duckdb.Connector
}

// NewEngine creates a new execution engine from a DuckDB connector.
func NewEngine(connector *duckdb.Connector) *Engine {
	return &Engine{connector: connector}
}

// arrowConn wraps a duckdb.Arrow with the underlying driver.Conn
// so both can be closed together.
type arrowConn struct {
	*duckdb.Arrow
	drv driver.Conn
}

func (a *arrowConn) Close() error {
	return a.drv.Close()
}

// Execute runs a SQL query and returns an Arrow RecordReader.
// The caller is responsible for releasing the reader.
func (e *Engine) Execute(ctx context.Context, query string) (array.RecordReader, error) {
	conn, err := e.connector.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	ar, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create arrow: %w", err)
	}

	ac := &arrowConn{Arrow: ar, drv: conn}

	reader, err := ac.QueryContext(ctx, query)
	if err != nil {
		ac.Close()
		return nil, fmt.Errorf("query: %w", err)
	}

	// Note: the connection stays open while the reader is being consumed.
	// The caller must release the reader. Connection cleanup happens via
	// the session's connection pool.
	return reader, nil
}

// Close closes the engine (no-op, connector is managed externally).
func (e *Engine) Close() error {
	return nil
}
