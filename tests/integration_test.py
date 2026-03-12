"""Integration tests for DuckDB Jupyter Kernel.

Requires a built and installed duckdb-kernel binary.
Run: python -m pytest tests/integration_test.py -v
"""

import os
import time

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

    def test_describe(self, kernel):
        displays, errors, _ = execute(kernel, ":describe test_users")
        assert not errors
        assert len(displays) == 1
        assert "id" in displays[0]
        assert "name" in displays[0]
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
        """Query results should produce Arrow IPC files in the spool directory."""
        displays, errors, _ = execute(kernel, "SELECT 1 AS val")
        assert not errors
        spool_base = "/tmp/duckdb-kernel"
        assert os.path.exists(spool_base), f"Spool directory not found: {spool_base}"
        sessions = os.listdir(spool_base)
        assert len(sessions) > 0, "No session directories in spool"
        session_dir = os.path.join(spool_base, sessions[0])
        arrow_files = [f for f in os.listdir(session_dir) if f.endswith(".arrow")]
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
