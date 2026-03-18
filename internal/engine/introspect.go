package engine

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Introspector executes DuckDB system queries for metadata introspection.
type Introspector struct {
	engine    *Engine
	sessionID string
}

// NewIntrospector creates a new introspector wrapping the given engine.
func NewIntrospector(eng *Engine, sessionID string) *Introspector {
	return &Introspector{engine: eng, sessionID: sessionID}
}

// Query executes an introspection query by type with optional filters.
func (i *Introspector) Query(ctx context.Context, typ, database, schema, table string) (any, error) {
	switch typ {
	case "session_info":
		return i.sessionInfo(ctx)
	case "databases":
		return i.queryToMaps(ctx, "SELECT database_name, database_oid, path, type, readonly, internal FROM duckdb_databases()")
	case "schemas":
		return i.queryToMaps(ctx, i.withFilter(
			"SELECT database_name, schema_name FROM duckdb_schemas()",
			"database_name", database))
	case "tables":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT table_name, column_count, estimated_size FROM duckdb_tables()",
			map[string]string{"database_name": database, "schema_name": schema}))
	case "views":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT view_name, column_count, sql FROM duckdb_views()",
			map[string]string{"database_name": database, "schema_name": schema}))
	case "functions":
		q := "SELECT function_name, function_type, parameters, parameter_types, return_type, varargs, macro_definition, has_side_effects, description FROM duckdb_functions() WHERE internal = false"
		if schema != "" {
			q += fmt.Sprintf(" AND schema_name = '%s'", escapeSingleQuote(schema))
		}
		return i.queryToMaps(ctx, q)
	case "system_functions":
		return i.queryToMaps(ctx, "SELECT function_name, function_type, parameters, parameter_types, return_type, varargs, macro_definition, has_side_effects, description FROM duckdb_functions() WHERE internal = true ORDER BY function_name")
	case "indexes":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT index_name, table_name, is_unique, sql FROM duckdb_indexes()",
			map[string]string{"database_name": database, "schema_name": schema, "table_name": table}))
	case "columns":
		return i.columns(ctx, database, schema, table)
	case "constraints":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT constraint_type, constraint_text, table_name FROM duckdb_constraints()",
			map[string]string{"database_name": database, "schema_name": schema, "table_name": table}))
	case "sequences":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT sequence_name, start_value, min_value, max_value, increment_by, cycle FROM duckdb_sequences()",
			map[string]string{"database_name": database, "schema_name": schema}))
	case "types":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT type_name, logical_type, type_category, type_size FROM duckdb_types()",
			map[string]string{"database_name": database, "schema_name": schema}))
	case "table_info":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT * FROM duckdb_tables()",
			map[string]string{"database_name": database, "schema_name": schema, "table_name": table}))
	case "view_info":
		return i.queryToMaps(ctx, i.withFilters(
			"SELECT * FROM duckdb_views()",
			map[string]string{"database_name": database, "schema_name": schema, "view_name": table}))
	case "database_info":
		return i.queryToMaps(ctx, i.withFilter(
			"SELECT * FROM duckdb_databases()",
			"database_name", database))
	case "describe":
		return i.describe(ctx, database, schema, table)
	case "summarize":
		return i.summarize(ctx, database, schema, table)
	case "secrets":
		return i.queryToMaps(ctx, "SELECT * FROM duckdb_secrets()")
	case "secret_types":
		return i.queryToMaps(ctx, "SELECT type, default_provider, extension FROM duckdb_secret_types()")
	case "settings":
		return i.queryToMaps(ctx, "SELECT name, value, description, input_type, scope FROM duckdb_settings()")
	case "extensions":
		return i.queryToMaps(ctx, "SELECT extension_name, loaded, installed, install_path, description, extension_version, install_mode, installed_from FROM duckdb_extensions()")
	case "memory":
		return i.queryToMaps(ctx, "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()")
	case "temporary_files":
		return i.queryToMaps(ctx, "SELECT path, size FROM duckdb_temporary_files()")
	case "variables":
		return i.queryToMaps(ctx, "SELECT name, value, type FROM duckdb_variables()")
	case "optimizers":
		return i.queryToMaps(ctx, "SELECT name FROM duckdb_optimizers()")
	case "prepared_statements":
		return i.queryToMaps(ctx, "SELECT name, statement_type FROM duckdb_prepared_statements()")
	case "logs":
		return i.queryToMaps(ctx, `
			SELECT l.timestamp, l.level AS log_level, l.message
			FROM duckdb_logs() l
			ORDER BY l.timestamp DESC
			LIMIT 100`)
	case "spool_files":
		return nil, fmt.Errorf("spool_files must be handled by the HTTP layer")
	default:
		return nil, fmt.Errorf("unknown type: %s", typ)
	}
}

// sessionInfo returns session metadata.
func (i *Introspector) sessionInfo(ctx context.Context) (map[string]any, error) {
	version := "unknown"
	reader, err := i.engine.Execute(ctx, "SELECT version() AS version")
	if err == nil {
		defer reader.Release()
		if reader.Next() {
			rec := reader.RecordBatch()
			if rec.NumRows() > 0 && rec.NumCols() > 0 {
				if col, ok := rec.Column(0).(*array.String); ok {
					version = strings.Clone(col.Value(0))
				}
			}
		}
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]any{
		"session_id":     i.sessionID,
		"duckdb_version": version,
		"kernel_mem_mb":  memStats.Sys / 1024 / 1024,
	}, nil
}

// columns queries information_schema.columns for structured column metadata.
func (i *Introspector) columns(ctx context.Context, database, schema, table string) ([]map[string]any, error) {
	if table == "" {
		return nil, fmt.Errorf("table parameter is required for columns")
	}
	q := "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE 1=1"
	if database != "" {
		q += fmt.Sprintf(" AND table_catalog = '%s'", escapeSingleQuote(database))
	}
	if schema != "" {
		q += fmt.Sprintf(" AND table_schema = '%s'", escapeSingleQuote(schema))
	}
	q += fmt.Sprintf(" AND table_name = '%s' ORDER BY ordinal_position", escapeSingleQuote(table))
	return i.queryToMaps(ctx, q)
}

// describe runs DESCRIBE on a fully-qualified table.
func (i *Introspector) describe(ctx context.Context, database, schema, table string) ([]map[string]any, error) {
	if table == "" {
		return nil, fmt.Errorf("table parameter is required for describe")
	}
	target := qualifiedTable(database, schema, table)
	return i.queryToMaps(ctx, fmt.Sprintf("DESCRIBE %s", target))
}

// summarize runs SUMMARIZE on a fully-qualified table.
func (i *Introspector) summarize(ctx context.Context, database, schema, table string) ([]map[string]any, error) {
	if table == "" {
		return nil, fmt.Errorf("table parameter is required for summarize")
	}
	target := qualifiedTable(database, schema, table)
	return i.queryToMaps(ctx, fmt.Sprintf("SUMMARIZE %s", target))
}

// queryToMaps executes a query and converts all rows to []map[string]any.
func (i *Introspector) queryToMaps(ctx context.Context, query string) ([]map[string]any, error) {
	reader, err := i.engine.Execute(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	fields := schema.Fields()
	var results []map[string]any

	for reader.Next() {
		rec := reader.RecordBatch()
		for rowIdx := int64(0); rowIdx < rec.NumRows(); rowIdx++ {
			row := make(map[string]any, len(fields))
			for colIdx, field := range fields {
				row[field.Name] = formatValue(rec.Column(colIdx), int(rowIdx), field)
			}
			results = append(results, row)
		}
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("reading records: %w", err)
	}

	if results == nil {
		results = []map[string]any{}
	}
	return results, nil
}

// withFilter adds a single WHERE clause to a query.
func (i *Introspector) withFilter(query, column, value string) string {
	if value == "" {
		return query
	}
	return fmt.Sprintf("%s WHERE %s = '%s'", query, column, escapeSingleQuote(value))
}

// withFilters adds multiple WHERE clauses to a query with deterministic ordering.
func (i *Introspector) withFilters(query string, filters map[string]string) string {
	// Sort keys for deterministic query generation.
	keys := make([]string, 0, len(filters))
	for col := range filters {
		keys = append(keys, col)
	}
	sort.Strings(keys)

	var conditions []string
	for _, col := range keys {
		if val := filters[col]; val != "" {
			conditions = append(conditions, fmt.Sprintf("%s = '%s'", col, escapeSingleQuote(val)))
		}
	}
	if len(conditions) == 0 {
		return query
	}
	return query + " WHERE " + strings.Join(conditions, " AND ")
}

// qualifiedTable builds a qualified table reference from parts using quoted identifiers.
func qualifiedTable(database, schema, table string) string {
	parts := []string{}
	if database != "" {
		parts = append(parts, quoteIdentifier(database))
	}
	if schema != "" {
		parts = append(parts, quoteIdentifier(schema))
	}
	parts = append(parts, quoteIdentifier(table))
	return strings.Join(parts, ".")
}

// quoteIdentifier wraps a SQL identifier in double quotes, escaping embedded double quotes.
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// escapeSingleQuote escapes single quotes for safe SQL string interpolation.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
