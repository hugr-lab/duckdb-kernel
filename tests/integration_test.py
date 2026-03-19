"""Integration tests for DuckDB Jupyter Kernel.

Requires a built and installed duckdb-kernel binary.
Run: python -m pytest tests/integration_test.py -v
"""

import json
import os
import tempfile
import time
import urllib.request

import jupyter_client
import pytest


@pytest.fixture(scope="module")
def kernel():
    """Start a DuckDB kernel for the test session."""
    km = jupyter_client.KernelManager(kernel_name="duckdb")
    km.start_kernel()
    kc = km.client()
    kc.start_channels()
    kc.wait_for_ready(timeout=30)
    # Flush any startup messages
    _flush(kc)
    yield kc
    kc.stop_channels()
    km.shutdown_kernel(now=True)


def _flush(kc, timeout=0.5):
    """Drain remaining iopub messages."""
    while True:
        try:
            kc.get_iopub_msg(timeout=timeout)
        except Exception:
            break


def execute(kc, code, timeout=10):
    """Execute code and collect all display_data and error messages until idle."""
    msg_id = kc.execute(code)
    displays = []
    errors = []
    display_mime_data = []  # Full MIME bundles from display_data
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            msg = kc.get_iopub_msg(timeout=2)
        except Exception:
            break
        if msg["parent_header"].get("msg_id") != msg_id:
            continue
        msg_type = msg["msg_type"]
        if msg_type == "display_data":
            displays.append(msg["content"]["data"].get("text/plain", ""))
            display_mime_data.append(msg["content"]["data"])
        elif msg_type == "error":
            errors.append(msg["content"]["evalue"])
        elif msg_type == "status" and msg["content"]["execution_state"] == "idle":
            break
    return displays, errors, display_mime_data


def _flush_shell(kc, timeout=0.5):
    """Drain remaining shell messages."""
    while True:
        try:
            kc.get_shell_msg(timeout=timeout)
        except Exception:
            break


def _get_base_url(kc):
    """Extract hugr_base_url from kernel_info_reply."""
    _flush(kc)
    _flush_shell(kc)
    msg_id = kc.kernel_info()
    reply = kc.get_shell_msg(timeout=10)
    _flush(kc)
    return reply["content"].get("hugr_base_url", "")


def introspect(base_url, typ, **params):
    """Call /introspect endpoint and return parsed JSON."""
    query = f"type={typ}"
    for k, v in params.items():
        if v:
            query += f"&{k}={v}"
    url = f"{base_url}/introspect?{query}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())


class TestKernelInfo:
    def test_kernel_ready(self, kernel):
        """Kernel starts and responds to kernel_info_request."""
        msg_id = kernel.kernel_info()
        reply = kernel.get_shell_msg(timeout=10)
        assert reply["msg_type"] == "kernel_info_reply"
        content = reply["content"]
        assert content["implementation"] == "duckdb-kernel"
        assert content["language_info"]["name"] == "sql"
        _flush(kernel)

    def test_hugr_base_url_in_kernel_info(self, kernel):
        """kernel_info_reply should contain hugr_base_url."""
        base_url = _get_base_url(kernel)
        assert base_url.startswith("http://127.0.0.1:"), f"Expected localhost URL, got: {base_url}"


class TestMetaCommands:
    def test_help(self, kernel):
        displays, errors, _ = execute(kernel, ":help")
        assert not errors
        assert len(displays) == 1
        assert ":help" in displays[0]
        assert ":tables" in displays[0]
        assert ":version" in displays[0]

    def test_version(self, kernel):
        displays, errors, _ = execute(kernel, ":version")
        assert not errors
        assert len(displays) == 1
        assert "DuckDB Kernel" in displays[0]
        assert "DuckDB v" in displays[0]

    def test_tables_empty(self, kernel):
        displays, errors, _ = execute(kernel, ":tables")
        assert not errors
        assert len(displays) == 1
        assert "SCHEMA" in displays[0]
        assert "NAME" in displays[0]

    def test_schemas(self, kernel):
        displays, errors, _ = execute(kernel, ":schemas")
        assert not errors
        assert len(displays) == 1
        assert "main" in displays[0]

    def test_limit(self, kernel):
        displays, errors, _ = execute(kernel, ":limit 10")
        assert not errors
        assert len(displays) == 1
        assert "10" in displays[0]

    def test_unknown_command(self, kernel):
        _, errors, _ = execute(kernel, ":nonexistent")
        assert len(errors) == 1
        assert "unknown" in errors[0].lower()


class TestSQLExecution:
    def test_select_literal(self, kernel):
        displays, errors, _ = execute(kernel, "SELECT 42 AS answer")
        assert not errors
        assert len(displays) == 1
        assert "42" in displays[0]
        assert "ANSWER" in displays[0]

    def test_create_insert_select(self, kernel):
        # CREATE
        displays, errors, _ = execute(kernel, "CREATE TABLE test_users (id INTEGER, name VARCHAR)")
        assert not errors

        # INSERT
        displays, errors, _ = execute(
            kernel,
            "INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        )
        assert not errors

        # SELECT
        displays, errors, _ = execute(kernel, "SELECT * FROM test_users ORDER BY id")
        assert not errors
        assert len(displays) == 1
        output = displays[0]
        assert "Alice" in output
        assert "Bob" in output
        assert "Charlie" in output

    def test_tables_after_create(self, kernel):
        displays, errors, _ = execute(kernel, ":tables")
        assert not errors
        assert "test_users" in displays[0]

    def test_describe_table(self, kernel):
        """DESCRIBE on a table shows columns with types."""
        execute(kernel, "CREATE TABLE IF NOT EXISTS test_users (id INTEGER, name VARCHAR)")
        displays, errors, _ = execute(kernel, ":describe test_users")
        assert not errors
        assert len(displays) == 1
        assert "id" in displays[0]
        assert "name" in displays[0]
        assert "INTEGER" in displays[0]
        assert "VARCHAR" in displays[0]

    def test_describe_query(self, kernel):
        """DESCRIBE on a SELECT query shows result column types."""
        displays, errors, _ = execute(kernel, ":describe SELECT 1 AS x, 'hello' AS y")
        assert not errors
        assert len(displays) == 1
        assert "x" in displays[0]
        assert "y" in displays[0]
        assert "INTEGER" in displays[0]
        assert "VARCHAR" in displays[0]

    def test_syntax_error(self, kernel):
        _, errors, _ = execute(kernel, "SELEC * FROM nonexistent")
        assert len(errors) == 1

    def test_empty_input(self, kernel):
        """Empty input should return ok with no output."""
        displays, errors, _ = execute(kernel, "")
        assert not errors
        assert not displays

    def test_preview_limit(self, kernel):
        """Results respect the preview limit."""
        execute(kernel, ":limit 2")
        execute(kernel, "CREATE TABLE test_limit AS SELECT range AS id FROM range(10)")
        displays, errors, _ = execute(kernel, "SELECT * FROM test_limit")
        assert not errors
        assert len(displays) == 1
        assert "10 rows total, showing 2" in displays[0]
        # Reset
        execute(kernel, ":limit 100")


class TestArrowSpool:
    def test_arrow_files_written(self, kernel):
        """Query results should produce Arrow IPC files in the flat spool directory."""
        displays, errors, _ = execute(kernel, "SELECT 1 AS val")
        assert not errors
        spool_base = os.path.join(tempfile.gettempdir(), "duckdb-kernel")
        assert os.path.exists(spool_base), f"Spool directory not found: {spool_base}"
        arrow_files = [f for f in os.listdir(spool_base) if f.endswith(".arrow")]
        assert len(arrow_files) > 0, "Expected at least one .arrow file in spool"


class TestMimeTypeEmission:
    """Tests for application/vnd.hugr.result+json MIME type output."""

    MIME_TYPE = "application/vnd.hugr.result+json"

    def test_mime_type_present(self, kernel):
        """Query results should include custom MIME type metadata."""
        _, errors, mime_data = execute(kernel, "SELECT 42 AS answer")
        assert not errors
        assert len(mime_data) == 1
        assert self.MIME_TYPE in mime_data[0], (
            f"Expected {self.MIME_TYPE} in display_data MIME bundle"
        )

    def test_mime_metadata_schema(self, kernel):
        """MIME metadata should contain query_id, arrow_url, rows, columns."""
        _, errors, mime_data = execute(kernel, "SELECT 1 AS id, 'hello' AS name")
        assert not errors
        meta = mime_data[0][self.MIME_TYPE]
        assert "query_id" in meta
        assert "arrow_url" in meta
        assert "rows" in meta
        assert "columns" in meta
        assert meta["rows"] == 1
        assert len(meta["columns"]) == 2
        assert meta["columns"][0]["name"] == "id"
        assert meta["columns"][1]["name"] == "name"

    def test_mime_arrow_url_format(self, kernel):
        """Arrow URL in MIME metadata should point to kernel HTTP server."""
        _, errors, mime_data = execute(kernel, "SELECT 100 AS val")
        assert not errors
        meta = mime_data[0][self.MIME_TYPE]
        arrow_url = meta["arrow_url"]
        assert "127.0.0.1" in arrow_url, f"Expected localhost URL, got: {arrow_url}"
        assert "/arrow" in arrow_url, f"Expected /arrow endpoint, got: {arrow_url}"

    def test_plain_text_always_present(self, kernel):
        """text/plain should always be present alongside custom MIME type."""
        _, errors, mime_data = execute(kernel, "SELECT 'test' AS col")
        assert not errors
        assert "text/plain" in mime_data[0]
        assert self.MIME_TYPE in mime_data[0]

    def test_no_mime_for_ddl(self, kernel):
        """DDL statements that produce no result columns should not emit custom MIME type."""
        _, errors, mime_data = execute(
            kernel, "CREATE TABLE IF NOT EXISTS mime_test_ddl (id INTEGER)"
        )
        assert not errors
        # CREATE TABLE returns no columns, so no display_data at all
        for data in mime_data:
            assert self.MIME_TYPE not in data

    def test_mime_row_count_matches(self, kernel):
        """MIME metadata rows should reflect actual total row count."""
        execute(kernel, ":limit 5")
        execute(
            kernel,
            "CREATE TABLE IF NOT EXISTS mime_rows AS SELECT range AS id FROM range(20)",
        )
        _, errors, mime_data = execute(kernel, "SELECT * FROM mime_rows")
        assert not errors
        meta = mime_data[0][self.MIME_TYPE]
        assert meta["rows"] == 20
        # Reset
        execute(kernel, ":limit 100")


class TestDefaultPreviewLimit:
    """Tests for the updated default preview limit of 100 rows."""

    def test_default_preview_limit_100(self, kernel):
        """Default preview should show up to 100 rows."""
        execute(
            kernel,
            "CREATE TABLE IF NOT EXISTS limit_test AS SELECT range AS id FROM range(200)",
        )
        displays, errors, _ = execute(kernel, "SELECT * FROM limit_test")
        assert not errors
        assert len(displays) == 1
        assert "200 rows total, showing 100" in displays[0]


class TestIntrospection:
    """Tests for the /introspect HTTP endpoint."""

    def test_missing_type_returns_400(self, kernel):
        """Missing type parameter should return 400."""
        base_url = _get_base_url(kernel)
        url = f"{base_url}/introspect"
        try:
            urllib.request.urlopen(url, timeout=5)
            assert False, "Expected HTTP error"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            body = json.loads(e.read())
            assert "missing type parameter" in body["error"]

    def test_unknown_type_returns_400(self, kernel):
        """Unknown type parameter should return 400."""
        base_url = _get_base_url(kernel)
        try:
            introspect(base_url, "nonexistent_type")
            assert False, "Expected HTTP error"
        except urllib.error.HTTPError as e:
            assert e.code == 400
            body = json.loads(e.read())
            assert "unknown type" in body["error"]

    def test_session_info(self, kernel):
        """session_info returns session_id, duckdb_version, kernel_mem_mb."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "session_info")
        assert resp["type"] == "session_info"
        data = resp["data"]
        assert "session_id" in data
        assert "duckdb_version" in data
        assert data["duckdb_version"].startswith("v")
        assert "kernel_mem_mb" in data

    def test_databases(self, kernel):
        """databases returns at least the default memory database."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "databases")
        assert resp["type"] == "databases"
        names = [d["database_name"] for d in resp["data"]]
        assert "memory" in names

    def test_schemas(self, kernel):
        """schemas returns main schema for memory database."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "schemas", database="memory")
        assert resp["type"] == "schemas"
        names = [s["schema_name"] for s in resp["data"]]
        assert "main" in names

    def test_tables_after_create(self, kernel):
        """tables returns created tables."""
        base_url = _get_base_url(kernel)
        execute(kernel, "CREATE TABLE IF NOT EXISTS intro_test (id INTEGER, name VARCHAR)")
        resp = introspect(base_url, "tables", database="memory", schema="main")
        assert resp["type"] == "tables"
        names = [t["table_name"] for t in resp["data"]]
        assert "intro_test" in names

    def test_columns(self, kernel):
        """columns returns column metadata for a table."""
        base_url = _get_base_url(kernel)
        execute(kernel, "CREATE TABLE IF NOT EXISTS intro_test (id INTEGER, name VARCHAR)")
        resp = introspect(base_url, "columns", database="memory", schema="main", table="intro_test")
        assert resp["type"] == "columns"
        col_names = [c["column_name"] for c in resp["data"]]
        assert "id" in col_names
        assert "name" in col_names

    def test_views(self, kernel):
        """views returns created views."""
        base_url = _get_base_url(kernel)
        execute(kernel, "CREATE TABLE IF NOT EXISTS intro_test (id INTEGER, name VARCHAR)")
        execute(kernel, "CREATE VIEW IF NOT EXISTS intro_view AS SELECT id FROM intro_test")
        resp = introspect(base_url, "views", database="memory", schema="main")
        assert resp["type"] == "views"
        names = [v["view_name"] for v in resp["data"]]
        assert "intro_view" in names

    def test_settings(self, kernel):
        """settings returns DuckDB configuration."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "settings")
        assert resp["type"] == "settings"
        assert len(resp["data"]) > 0
        names = [s["name"] for s in resp["data"]]
        assert "threads" in names

    def test_extensions(self, kernel):
        """extensions returns extension list."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "extensions")
        assert resp["type"] == "extensions"
        assert isinstance(resp["data"], list)

    def test_memory(self, kernel):
        """memory returns DuckDB buffer manager info."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "memory")
        assert resp["type"] == "memory"
        assert isinstance(resp["data"], list)

    def test_spool_files(self, kernel):
        """spool_files returns Arrow result files after a query."""
        base_url = _get_base_url(kernel)
        execute(kernel, "SELECT 1 AS spool_test")
        resp = introspect(base_url, "spool_files")
        assert resp["type"] == "spool_files"
        assert len(resp["data"]) > 0
        assert "query_id" in resp["data"][0]
        assert "size_bytes" in resp["data"][0]

    def test_describe(self, kernel):
        """describe returns DESCRIBE output for a table."""
        base_url = _get_base_url(kernel)
        execute(kernel, "CREATE TABLE IF NOT EXISTS intro_test (id INTEGER, name VARCHAR)")
        resp = introspect(base_url, "describe", database="memory", schema="main", table="intro_test")
        assert resp["type"] == "describe"
        assert len(resp["data"]) > 0
        col_names = [c["column_name"] for c in resp["data"]]
        assert "id" in col_names

    def test_summarize(self, kernel):
        """summarize returns SUMMARIZE output for a table."""
        base_url = _get_base_url(kernel)
        execute(kernel, "CREATE TABLE IF NOT EXISTS intro_test (id INTEGER, name VARCHAR)")
        execute(kernel, "INSERT INTO intro_test VALUES (1, 'a') ON CONFLICT DO NOTHING")
        resp = introspect(base_url, "summarize", database="memory", schema="main", table="intro_test")
        assert resp["type"] == "summarize"
        assert len(resp["data"]) > 0

    def test_secrets(self, kernel):
        """secrets returns list without credential values."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "secrets")
        assert resp["type"] == "secrets"
        assert isinstance(resp["data"], list)
        # Verify no credential fields leak through.
        for s in resp["data"]:
            assert "secret" not in s, "Credential values should not be exposed"
            assert "token" not in s, "Token values should not be exposed"

    def test_secret_types(self, kernel):
        """secret_types returns available secret types."""
        base_url = _get_base_url(kernel)
        resp = introspect(base_url, "secret_types")
        assert resp["type"] == "secret_types"
        assert isinstance(resp["data"], list)


class TestMimeTimingMetadata:
    """Tests for data_size_bytes, query_time_ms, transfer_time_ms in MIME metadata."""

    MIME_TYPE = "application/vnd.hugr.result+json"

    def test_timing_fields_present(self, kernel):
        """MIME metadata should contain timing and size fields."""
        _, errors, mime_data = execute(kernel, "SELECT 42 AS val")
        assert not errors
        meta = mime_data[0][self.MIME_TYPE]
        assert "data_size_bytes" in meta
        assert "query_time_ms" in meta
        assert "transfer_time_ms" in meta
        assert meta["data_size_bytes"] > 0
        assert meta["query_time_ms"] >= 0
        assert meta["transfer_time_ms"] >= 0

    def test_kernel_mem_removed(self, kernel):
        """kernel_mem_mb should no longer be in MIME metadata."""
        _, errors, mime_data = execute(kernel, "SELECT 1 AS val")
        assert not errors
        meta = mime_data[0][self.MIME_TYPE]
        assert "kernel_mem_mb" not in meta


class TestExplainCommand:
    """Tests for :explain meta command."""

    def test_explain_returns_html(self, kernel):
        """":explain SELECT ..." should return HTML output."""
        execute(kernel, "CREATE TABLE IF NOT EXISTS explain_test AS SELECT range AS id FROM range(100)")
        _, errors, mime_data = execute(kernel, ":explain SELECT * FROM explain_test WHERE id > 50")
        assert not errors
        assert len(mime_data) == 1
        assert "text/html" in mime_data[0]
        html = mime_data[0]["text/html"]
        assert "<" in html  # Basic HTML check

    def test_explain_missing_sql(self, kernel):
        """":explain" without SQL should return an error."""
        _, errors, _ = execute(kernel, ":explain")
        assert len(errors) == 1
        assert "usage" in errors[0].lower()

    def test_explain_plain_text_fallback(self, kernel):
        """":explain" should also include text/plain fallback."""
        execute(kernel, "CREATE TABLE IF NOT EXISTS explain_test AS SELECT range AS id FROM range(10)")
        _, errors, mime_data = execute(kernel, ":explain SELECT * FROM explain_test")
        assert not errors
        assert len(mime_data) == 1
        assert "text/plain" in mime_data[0]
