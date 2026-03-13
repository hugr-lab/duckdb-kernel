package meta

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/hugr-lab/duckdb-kernel/internal/engine"
	"github.com/hugr-lab/duckdb-kernel/internal/renderer"
)

// CommandResult holds the output of a meta command with optional MIME type.
type CommandResult struct {
	Text     string // Plain text output
	HTML     string // HTML output (if non-empty, sent as text/html)
}

// CommandHandler is a function that handles a meta command.
type CommandHandler func(ctx context.Context, args []string) (string, error)

// RichCommandHandler returns a CommandResult with optional HTML output.
type RichCommandHandler func(ctx context.Context, args []string) (*CommandResult, error)

// Registry holds registered meta commands.
type Registry struct {
	commands     map[string]CommandHandler
	richCommands map[string]RichCommandHandler
	descriptions map[string]string
	eng          *engine.Engine
	setLimit     func(int)
}

// NewRegistry creates a new command registry with default commands.
func NewRegistry(eng *engine.Engine, setLimit func(int)) *Registry {
	r := &Registry{
		commands:     make(map[string]CommandHandler),
		richCommands: make(map[string]RichCommandHandler),
		descriptions: make(map[string]string),
		eng:          eng,
		setLimit:     setLimit,
	}
	r.registerDefaults()
	return r
}

func (r *Registry) registerDefaults() {
	r.Register("help", "Show available commands", r.handleHelp)
	r.Register("version", "Show kernel and DuckDB version", r.handleVersion)
	r.Register("tables", "List all tables", r.handleTables)
	r.Register("schemas", "List all schemas", r.handleSchemas)
	r.Register("describe", "Show columns and types (DESCRIBE)", r.handleDescribe)
	r.RegisterRich("explain", "Show query execution plan (EXPLAIN ANALYZE)", r.handleExplain)
	r.Register("limit", "Set preview row limit", r.handleLimit)
}

// Register adds a command to the registry.
func (r *Registry) Register(name, description string, handler CommandHandler) {
	r.commands[name] = handler
	r.descriptions[name] = description
}

// RegisterRich adds a rich command (with HTML support) to the registry.
func (r *Registry) RegisterRich(name, description string, handler RichCommandHandler) {
	r.richCommands[name] = handler
	r.descriptions[name] = description
}

// IsMeta returns true if the input starts with ':'.
func IsMeta(input string) bool {
	return strings.HasPrefix(strings.TrimSpace(input), ":")
}

// Dispatch parses and executes a meta command, returning a CommandResult.
func (r *Registry) Dispatch(ctx context.Context, input string) (*CommandResult, error) {
	input = strings.TrimSpace(input)
	input = strings.TrimPrefix(input, ":")

	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	name := parts[0]
	args := parts[1:]

	// Check rich commands first.
	if richHandler, ok := r.richCommands[name]; ok {
		return richHandler(ctx, args)
	}

	handler, ok := r.commands[name]
	if !ok {
		return nil, fmt.Errorf("Unknown command: :%s\nType :help for available commands", name)
	}

	text, err := handler(ctx, args)
	if err != nil {
		return nil, err
	}
	return &CommandResult{Text: text}, nil
}

func (r *Registry) handleHelp(_ context.Context, _ []string) (string, error) {
	var sb strings.Builder
	sb.WriteString("Available commands:\n")
	sb.WriteString("  :help               Show this help message\n")
	sb.WriteString("  :version            Show kernel and DuckDB version\n")
	sb.WriteString("  :tables             List all tables\n")
	sb.WriteString("  :schemas            List all schemas\n")
	sb.WriteString("  :describe <table|query>  Show columns and types (DESCRIBE)\n")
	sb.WriteString("  :explain <SQL>      Show query execution plan (EXPLAIN ANALYZE)\n")
	sb.WriteString("  :limit <n>          Set preview row limit\n")
	return sb.String(), nil
}

func (r *Registry) handleVersion(_ context.Context, _ []string) (string, error) {
	// Query DuckDB version
	ctx := context.Background()
	reader, err := r.eng.Execute(ctx, "SELECT version() AS version")
	if err != nil {
		return fmt.Sprintf("DuckDB Kernel v%s\nDuckDB version: unknown", "0.1.0"), nil
	}
	defer reader.Release()

	version := "unknown"
	if reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > 0 && rec.NumCols() > 0 {
			col, ok := rec.Column(0).(*array.String)
			if ok {
				version = col.Value(0)
			}
		}
	}

	return fmt.Sprintf("DuckDB Kernel v%s\nDuckDB %s", "0.1.0", version), nil
}

func (r *Registry) handleTables(ctx context.Context, _ []string) (string, error) {
	query := `SELECT table_schema AS schema, table_name AS name, table_type AS type
FROM information_schema.tables
ORDER BY table_schema, table_name`
	return r.executeAndRender(ctx, query)
}

func (r *Registry) handleSchemas(ctx context.Context, _ []string) (string, error) {
	query := `SELECT schema_name FROM information_schema.schemata ORDER BY schema_name`
	return r.executeAndRender(ctx, query)
}

func (r *Registry) handleDescribe(ctx context.Context, args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("Usage: :describe <table_name or query>")
	}
	target := strings.Join(args, " ")
	query := fmt.Sprintf("DESCRIBE %s", target)
	return r.executeAndRender(ctx, query)
}

func (r *Registry) handleExplain(ctx context.Context, args []string) (*CommandResult, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Usage: :explain <SQL query>")
	}
	sql := strings.Join(args, " ")
	query := fmt.Sprintf("EXPLAIN (FORMAT 'html', ANALYZE) (%s)", sql)

	reader, err := r.eng.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	// EXPLAIN (FORMAT 'html') returns explain_key, explain_value columns.
	// The HTML is in explain_value.
	var html string
	if reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > 0 && rec.NumCols() >= 2 {
			if col, ok := rec.Column(1).(*array.String); ok {
				html = strings.Clone(col.Value(0))
			} else if col, ok := rec.Column(1).(*array.LargeString); ok {
				html = strings.Clone(col.Value(0))
			}
		}
	}

	if html == "" {
		return &CommandResult{Text: "No execution plan available"}, nil
	}

	return &CommandResult{
		Text: fmt.Sprintf("Query plan for: %s", sql),
		HTML: html,
	}, nil
}

func (r *Registry) handleLimit(_ context.Context, args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("Usage: :limit <n>")
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n <= 0 {
		return "", fmt.Errorf("Usage: :limit <n> (n must be a positive integer)")
	}
	r.setLimit(n)
	return fmt.Sprintf("Preview limit set to %d rows", n), nil
}

func (r *Registry) executeAndRender(ctx context.Context, query string) (string, error) {
	reader, err := r.eng.Execute(ctx, query)
	if err != nil {
		return "", err
	}
	defer reader.Release()

	result, err := engine.BuildPreviewFromReader(reader, 1000, nil)
	if err != nil {
		return "", err
	}

	if len(result.Columns) == 0 {
		return "No results", nil
	}

	columns := make([]string, len(result.Columns))
	for i, col := range result.Columns {
		columns[i] = col.Name
	}

	return renderer.RenderTable(columns, result.Rows), nil
}
