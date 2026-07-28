"""Process-wide isolation for the e2e suite.

The e2e tests spin up real masters and clients in-process; both sides
persist state under the XDG config home (node identity file, RSA .pem
files, Noise static keys and TOFU key pins). Redirect XDG before pytest
imports the test modules so import-time configuration constants cannot
resolve to the developer's real ``~/.config/hivemind``.
"""

import json
import os
from pathlib import Path
import tempfile

import pytest


_xdg_tempdir = None


def pytest_configure(config):
    """Isolate persistent state before test collection imports HiveMind."""
    global _xdg_tempdir
    _xdg_tempdir = tempfile.TemporaryDirectory(
        prefix="hivemind-websocket-client-e2e-"
    )
    root = Path(_xdg_tempdir.name)
    os.environ["XDG_CONFIG_HOME"] = str(root / "config")
    os.environ["XDG_DATA_HOME"] = str(root / "data")
    os.environ["XDG_CACHE_HOME"] = str(root / "cache")

    # Successful scenarios use strong credentials. Keep the production
    # strength backstop enabled so weak-password tests exercise the real
    # client/server policy instead of a monkeypatched approximation.
    os.environ["HIVEMIND_DISABLE_PASSWORD_STRENGTH_CHECK"] = "0"

    # Non-Noise connections top out at protocol v1, so the legacy-fallback
    # matrix rows need the server's protocol floor below the production
    # default (min_protocol_version=2). Only this isolated process config is
    # lowered; the shipped default is unchanged.
    server_cfg_dir = root / "config" / "hivemind-core"
    server_cfg_dir.mkdir(parents=True, exist_ok=True)
    (server_cfg_dir / "server.json").write_text(
        json.dumps({"min_protocol_version": 0}))


@pytest.fixture(autouse=True)
def isolated_identity_storage(monkeypatch):
    """Force NodeIdentity below the process-wide temporary config root."""
    import hivemind_bus_client.identity as identity_module
    from json_database import JsonConfigXDG

    # JsonConfigXDG binds its default xdg_folder when json_database is
    # imported, before pytest_configure changes the environment. Patch the
    # constructor used by NodeIdentity so collection order can never redirect
    # an e2e client back to the developer's real identity file.
    identity_config_root = Path(os.environ["XDG_CONFIG_HOME"])

    def isolated_identity_config(name, *args, **kwargs):
        """Construct an identity config below this test's XDG root."""
        kwargs["xdg_folder"] = identity_config_root
        return JsonConfigXDG(name, *args, **kwargs)

    monkeypatch.setattr(
        identity_module, "JsonConfigXDG", isolated_identity_config
    )


def pytest_unconfigure(config):
    """Remove the exact temporary tree created for this pytest process."""
    global _xdg_tempdir
    if _xdg_tempdir is not None:
        _xdg_tempdir.cleanup()
        _xdg_tempdir = None
