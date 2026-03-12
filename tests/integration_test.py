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
        elif msg_type == "error":
            errors.append(msg["content"]["evalue"])
        elif msg_type == "status" and msg["content"]["execution_state"] == "idle":
            break
    return displays, errors


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
        displays, errors = execute(kernel, ":help")
        assert not errors
        assert len(displays) == 1
        assert ":help" in displays[0]
        assert ":tables" in displays[0]
        assert ":version" in displays[0]

    def test_version(self, kernel):
        displays, errors = execute(kernel, ":version")
        assert not errors
        assert len(displays) == 1
        assert "DuckDB Kernel" in displays[0]
        assert "DuckDB v" in displays[0]

    def test_tables_empty(self, kernel):
        displays, errors = execute(kernel, ":tables")
        assert not errors
        assert len(displays) == 1
        assert "SCHEMA" in displays[0]
        assert "NAME" in displays[0]

    def test_schemas(self, kernel):
        displays, errors = execute(kernel, ":schemas")
        assert not errors
        assert len(displays) == 1
        assert "main" in displays[0]

    def test_limit(self, kernel):
        displays, errors = execute(kernel, ":limit 10")
        assert not errors
        assert len(displays) == 1
        assert "10" in displays[0]

    def test_unknown_command(self, kernel):
        _, errors = execute(kernel, ":nonexistent")
        assert len(errors) == 1
        assert "unknown" in errors[0].lower()


class TestSQLExecution:
    def test_select_literal(self, kernel):
        displays, errors = execute(kernel, "SELECT 42 AS answer")
        assert not errors
        assert len(displays) == 1
        assert "42" in displays[0]
        assert "ANSWER" in displays[0]

    def test_create_insert_select(self, kernel):
        # CREATE
        displays, errors = execute(kernel, "CREATE TABLE test_users (id INTEGER, name VARCHAR)")
        assert not errors

        # INSERT
        displays, errors = execute(
            kernel,
            "INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        )
        assert not errors

        # SELECT
        displays, errors = execute(kernel, "SELECT * FROM test_users ORDER BY id")
        assert not errors
        assert len(displays) == 1
        output = displays[0]
        assert "Alice" in output
        assert "Bob" in output
        assert "Charlie" in output

    def test_tables_after_create(self, kernel):
        displays, errors = execute(kernel, ":tables")
        assert not errors
        assert "test_users" in displays[0]

    def test_describe(self, kernel):
        displays, errors = execute(kernel, ":describe test_users")
        assert not errors
        assert len(displays) == 1
        assert "id" in displays[0]
        assert "name" in displays[0]
        assert "INTEGER" in displays[0]
        assert "VARCHAR" in displays[0]

    def test_syntax_error(self, kernel):
        _, errors = execute(kernel, "SELEC * FROM nonexistent")
        assert len(errors) == 1

    def test_empty_input(self, kernel):
        """Empty input should return ok with no output."""
        displays, errors = execute(kernel, "")
        assert not errors
        assert not displays

    def test_preview_limit(self, kernel):
        """Results respect the preview limit."""
        execute(kernel, ":limit 2")
        execute(kernel, "CREATE TABLE test_limit AS SELECT range AS id FROM range(10)")
        displays, errors = execute(kernel, "SELECT * FROM test_limit")
        assert not errors
        assert len(displays) == 1
        assert "10 rows total, showing 2" in displays[0]
        # Reset
        execute(kernel, ":limit 50")


class TestArrowSpool:
    def test_arrow_files_written(self, kernel):
        """Query results should produce Arrow IPC files in the spool directory."""
        displays, errors = execute(kernel, "SELECT 1 AS val")
        assert not errors
        spool_base = "/tmp/duckdb-kernel"
        if os.path.exists(spool_base):
            sessions = os.listdir(spool_base)
            if sessions:
                session_dir = os.path.join(spool_base, sessions[0])
                arrow_files = [f for f in os.listdir(session_dir) if f.endswith(".arrow")]
                assert len(arrow_files) > 0, "Expected at least one .arrow file in spool"
