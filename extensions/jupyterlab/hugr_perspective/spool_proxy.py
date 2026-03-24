"""Arrow Spool Proxy — serves Arrow IPC results from kernel spool directories.

Reads Arrow IPC files directly from disk, optionally replaces geometry columns
with placeholder strings, supports column projection. Streams response
chunk-by-chunk without accumulating data in memory.

Endpoints:
  GET /hugr/spool/arrow/stream?kernel_id=X&q=Y[&geoarrow=1][&columns=a,b]
  GET /hugr/spool/arrow?kernel_id=X&q=Y
  POST /hugr/spool/pin?kernel_id=X&q=Y&dir=/path
  POST /hugr/spool/unpin?q=Y&dir=/path
  GET /hugr/spool/is_pinned?q=Y&dir=/path
  GET /hugr/spool/delete?kernel_id=X&q=Y
"""
import io
import os
import shutil
import struct
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import tornado.web
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.utils import url_path_join

GEO_PLACEHOLDER = "{geometry}"

# Kernel spool directories under system temp
KERNEL_SPOOL_DIRS = {
    "hugr-kernel": "hugr-kernel",
    "duckdb-kernel": "duckdb-kernel",
}

# Pin result subdirectory names per kernel type
KERNEL_PIN_DIRS = {
    "hugr-kernel": "hugr-results",
    "duckdb-kernel": "duckdb-results",
}


def _spool_base() -> str:
    """Return system temp directory (matches Go os.TempDir())."""
    return tempfile.gettempdir()


def _find_spool_file(kernel_id: str, query_id: str) -> str | None:
    """Find Arrow spool file for a given kernel and query.

    Searches all kernel spool directories for the file. Uses kernel_id
    to narrow down the search — kernel_id may be a session subdirectory.
    """
    base = _spool_base()
    safe_query = Path(query_id).name  # prevent path traversal

    for kernel_type, spool_dir_name in KERNEL_SPOOL_DIRS.items():
        spool_root = os.path.join(base, spool_dir_name)
        if not os.path.isdir(spool_root):
            continue

        # Check direct: {spool_root}/{query_id}.arrow
        direct = os.path.join(spool_root, f"{safe_query}.arrow")
        if os.path.isfile(direct):
            return direct

        # Check with session subdirs: {spool_root}/{session_id}/{query_id}.arrow
        for entry in os.scandir(spool_root):
            if entry.is_dir():
                path = os.path.join(entry.path, f"{safe_query}.arrow")
                if os.path.isfile(path):
                    return path

    return None


def _find_pin_path(query_id: str, notebook_dir: str) -> str | None:
    """Find pinned result file in notebook directory."""
    safe_query = Path(query_id).name
    for pin_dir_name in KERNEL_PIN_DIRS.values():
        path = os.path.join(notebook_dir, pin_dir_name, f"{safe_query}.arrow")
        if os.path.isfile(path):
            return path
    return None


def _detect_kernel_type(kernel_id: str, query_id: str) -> str:
    """Detect kernel type from spool file location."""
    base = _spool_base()
    safe_query = Path(query_id).name

    for kernel_type, spool_dir_name in KERNEL_SPOOL_DIRS.items():
        spool_root = os.path.join(base, spool_dir_name)
        if not os.path.isdir(spool_root):
            continue
        direct = os.path.join(spool_root, f"{safe_query}.arrow")
        if os.path.isfile(direct):
            return kernel_type
        for entry in os.scandir(spool_root):
            if entry.is_dir():
                path = os.path.join(entry.path, f"{safe_query}.arrow")
                if os.path.isfile(path):
                    return kernel_type

    return "duckdb-kernel"  # fallback


def _is_geo_column(field: pa.Field) -> bool:
    """Detect geometry columns via Arrow extension metadata or type heuristics."""
    # Check GeoArrow extension metadata (authoritative)
    meta = field.metadata or {}
    ext_name = meta.get(b"ARROW:extension:name", b"").decode()
    if ext_name.startswith("geoarrow.") or ext_name.startswith("ogc."):
        return True

    t = field.type
    # WKB encoding: binary/large_binary with geo-like field name
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        name = field.name.lower()
        return name in ("geometry", "geom", "wkb_geometry", "the_geom", "shape")

    # GeoArrow native: fixed_size_list<double>[2..4] (Point)
    if pa.types.is_fixed_size_list(t):
        if pa.types.is_floating(t.value_type) and t.list_size in (2, 3, 4):
            name = field.name.lower()
            return name in ("geometry", "geom", "the_geom", "shape", "coordinates")

    # GeoArrow native: list<fixed_size_list<...>> (LineString) or list<list<...>> (Polygon)
    if pa.types.is_list(t):
        inner = t.value_type
        if pa.types.is_fixed_size_list(inner) or pa.types.is_list(inner):
            name = field.name.lower()
            return name in ("geometry", "geom", "the_geom", "shape", "coordinates")

    return False


class SpoolStreamHandler(JupyterHandler):
    """GET /hugr/spool/arrow/stream — stream Arrow IPC with optional geo replacement."""

    @tornado.web.authenticated
    async def get(self):
        kernel_id = self.get_argument("kernel_id", "")
        query_id = self.get_argument("q", "")
        geoarrow = self.get_argument("geoarrow", None) is not None
        columns = self.get_argument("columns", "")
        limit = self.get_argument("limit", "")

        if not query_id:
            self.set_status(400)
            self.write({"error": "q parameter required"})
            return

        # Find spool file or pinned file
        path = _find_spool_file(kernel_id, query_id)
        if not path:
            self.set_status(404)
            self.write({"error": "result not found"})
            return

        self.set_header("Content-Type", "application/octet-stream")
        self.set_header("Cache-Control", "no-cache")

        try:
            reader = ipc.open_stream(pa.OSFile(path, "rb"))
        except Exception as e:
            self.set_status(500)
            self.write({"error": f"failed to open Arrow file: {e}"})
            return

        # Detect geo columns and build output schema
        do_geo_replace = not geoarrow
        geo_indices = []
        out_fields = []
        col_indices = None

        for i, field in enumerate(reader.schema):
            if do_geo_replace and _is_geo_column(field):
                geo_indices.append(i)
                out_fields.append(pa.field(field.name, pa.string()))
            else:
                out_fields.append(field)

        out_schema = pa.schema(out_fields)

        # Column projection
        if columns:
            col_names = set(columns.split(","))
            col_indices = [i for i, f in enumerate(out_schema) if f.name in col_names]
            out_schema = pa.schema([out_schema.field(i) for i in col_indices])

        # Row limit
        row_limit = int(limit) if limit else 0
        rows_sent = 0

        for batch in reader:
            if row_limit and rows_sent >= row_limit:
                break

            # Trim batch if needed
            if row_limit and rows_sent + batch.num_rows > row_limit:
                batch = batch.slice(0, row_limit - rows_sent)

            # Geo replacement
            if geo_indices:
                new_columns = []
                for i in range(batch.num_columns):
                    if i in geo_indices:
                        new_columns.append(
                            pa.nulls(batch.num_rows, type=pa.string()).fill_null(GEO_PLACEHOLDER)
                        )
                    else:
                        new_columns.append(batch.column(i))

                fields = []
                for i, field in enumerate(batch.schema):
                    if i in geo_indices:
                        fields.append(pa.field(field.name, pa.string()))
                    else:
                        fields.append(field)

                batch = pa.record_batch(new_columns, schema=pa.schema(fields))

            # Column projection
            if col_indices is not None:
                batch = pa.record_batch(
                    [batch.column(i) for i in col_indices],
                    schema=out_schema,
                )

            # Serialize to IPC
            buf = io.BytesIO()
            writer = ipc.new_stream(buf, out_schema)
            writer.write_batch(batch)
            writer.close()
            chunk = buf.getvalue()

            # Write length-prefixed chunk
            self.write(struct.pack("<I", len(chunk)))
            self.write(chunk)
            self.flush()

            rows_sent += batch.num_rows

        # End marker
        self.write(struct.pack("<I", 0))
        self.flush()
        self.finish()


class SpoolRawHandler(JupyterHandler):
    """GET /hugr/spool/arrow — raw Arrow IPC file download."""

    @tornado.web.authenticated
    async def get(self):
        kernel_id = self.get_argument("kernel_id", "")
        query_id = self.get_argument("q", "")

        if not query_id:
            self.set_status(400)
            self.write({"error": "q parameter required"})
            return

        path = _find_spool_file(kernel_id, query_id)
        if not path:
            self.set_status(404)
            self.write({"error": "result not found"})
            return

        self.set_header("Content-Type", "application/vnd.apache.arrow.stream")
        self.set_header("Content-Disposition", f'attachment; filename="{query_id}.arrow"')

        # Stream file in chunks
        with open(path, "rb") as f:
            while True:
                chunk = f.read(256 * 1024)
                if not chunk:
                    break
                self.write(chunk)
                self.flush()
        self.finish()


class SpoolPinHandler(JupyterHandler):
    """POST /hugr/spool/pin — pin result to notebook directory."""

    @tornado.web.authenticated
    async def post(self):
        kernel_id = self.get_argument("kernel_id", "")
        query_id = self.get_argument("q", "")
        notebook_dir = self.get_argument("dir", "")

        if not query_id or not notebook_dir:
            self.set_status(400)
            self.write({"error": "q and dir parameters required"})
            return

        src = _find_spool_file(kernel_id, query_id)
        if not src:
            self.set_status(404)
            self.write({"error": "result not found"})
            return

        resolved_dir = _resolve_notebook_dir(self, notebook_dir)
        kernel_type = _detect_kernel_type(kernel_id, query_id)
        pin_dir_name = KERNEL_PIN_DIRS.get(kernel_type, "results")
        pin_dir = os.path.join(resolved_dir, pin_dir_name)
        safe_query = Path(query_id).name

        os.makedirs(pin_dir, exist_ok=True)

        # Create .gitignore if not exists
        gitignore = os.path.join(pin_dir, ".gitignore")
        if not os.path.exists(gitignore):
            with open(gitignore, "w") as f:
                f.write("*.arrow\n")

        dst = os.path.join(pin_dir, f"{safe_query}.arrow")
        shutil.copy2(src, dst)

        self.write({"ok": True, "path": dst})


class SpoolUnpinHandler(JupyterHandler):
    """POST /hugr/spool/unpin — remove pinned result from notebook directory."""

    @tornado.web.authenticated
    async def post(self):
        query_id = self.get_argument("q", "")
        notebook_dir = self.get_argument("dir", "")

        if not query_id or not notebook_dir:
            self.set_status(400)
            self.write({"error": "q and dir parameters required"})
            return

        resolved_dir = _resolve_notebook_dir(self, notebook_dir)
        path = _find_pin_path(query_id, resolved_dir)
        if path and os.path.exists(path):
            os.remove(path)
            self.write({"ok": True})
        else:
            self.set_status(404)
            self.write({"error": "pinned result not found"})


class SpoolIsPinnedHandler(JupyterHandler):
    """GET /hugr/spool/is_pinned — check if result is pinned."""

    @tornado.web.authenticated
    async def get(self):
        query_id = self.get_argument("q", "")
        notebook_dir = self.get_argument("dir", "")

        if not query_id:
            self.set_status(400)
            self.write({"error": "q parameter required"})
            return

        # Without dir, can't check pin status — return false
        if not notebook_dir:
            self.write({"pinned": False})
            return

        resolved_dir = _resolve_notebook_dir(self, notebook_dir)
        path = _find_pin_path(query_id, resolved_dir)
        self.write({"pinned": path is not None})


class SpoolDeleteHandler(JupyterHandler):
    """DELETE /hugr/spool/delete — delete spool result and pinned copy."""

    @tornado.web.authenticated
    async def delete(self):
        kernel_id = self.get_argument("kernel_id", "")
        query_id = self.get_argument("q", "")
        notebook_dir = self.get_argument("dir", "")

        if not query_id:
            self.set_status(400)
            self.write({"error": "q parameter required"})
            return

        deleted = False

        # Delete from spool (temp dir)
        path = _find_spool_file(kernel_id, query_id)
        if path and os.path.exists(path):
            os.remove(path)
            deleted = True

        # Delete pinned copy from notebook dir
        if notebook_dir:
            resolved_dir = _resolve_notebook_dir(self, notebook_dir)
            pin_path = _find_pin_path(query_id, resolved_dir)
            if pin_path and os.path.exists(pin_path):
                os.remove(pin_path)
                deleted = True

        if deleted:
            self.write({"ok": True})
        else:
            self.set_status(404)
            self.write({"error": "result not found"})


def _resolve_notebook_dir(handler: JupyterHandler, relative_dir: str) -> str:
    """Resolve notebook directory relative to Jupyter server root."""
    root = handler.settings.get("server_root_dir", "")
    if not root:
        root = handler.settings.get("root_dir", os.path.expanduser("~"))
    return os.path.realpath(os.path.expanduser(os.path.join(root, relative_dir)))


def setup_handlers(web_app):
    """Register spool proxy routes."""
    host_pattern = ".*$"
    base_url = web_app.settings["base_url"]
    route = lambda pattern: url_path_join(base_url, "hugr", pattern)

    web_app.add_handlers(host_pattern, [
        (route("spool/arrow/stream"), SpoolStreamHandler),
        (route("spool/arrow"), SpoolRawHandler),
        (route("spool/pin"), SpoolPinHandler),
        (route("spool/unpin"), SpoolUnpinHandler),
        (route("spool/is_pinned"), SpoolIsPinnedHandler),
        (route("spool/delete"), SpoolDeleteHandler),
    ])
