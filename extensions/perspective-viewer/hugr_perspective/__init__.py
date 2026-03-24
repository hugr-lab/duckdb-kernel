"""HUGR Perspective Viewer - Jupyter Lab Extension."""

import os

HERE = os.path.abspath(os.path.dirname(__file__))


def _jupyter_labextension_paths():
    return [{"src": os.path.join(HERE, "labextension"), "dest": "@hugr-lab/perspective-viewer"}]


def _jupyter_server_extension_points():
    return [{"module": "hugr_perspective"}]


def _load_jupyter_server_extension(serverapp):
    """Register the HUGR Perspective Viewer extension with spool proxy routes."""
    from .spool_proxy import setup_handlers
    setup_handlers(serverapp.web_app)
    serverapp.log.info("HUGR Perspective Viewer extension loaded (spool proxy enabled)")
