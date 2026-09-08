"""WebSocket keepalive configuration shared by HiveMind clients."""

from __future__ import annotations

import math
import os
from typing import Optional, Sequence

from ovos_utils.log import LOG


DEFAULT_WEBSOCKET_PING_INTERVAL = 25.0
DEFAULT_WEBSOCKET_PING_TIMEOUT = 10.0

CLIENT_PING_INTERVAL_ENVS = (
    "HIVEMIND_WEBSOCKET_CLIENT_PING_INTERVAL",
    "HIVEMIND_WEBSOCKET_PING_INTERVAL",
)
CLIENT_PING_TIMEOUT_ENVS = (
    "HIVEMIND_WEBSOCKET_CLIENT_PING_TIMEOUT",
    "HIVEMIND_WEBSOCKET_PING_TIMEOUT",
)


def _first_env_value(names: Sequence[str]) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return None


def _non_negative_float(value, default: float, label: str) -> float:
    if value in (None, ""):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        LOG.warning(f"Ignoring invalid {label}: {value!r}")
        return default
    if not math.isfinite(parsed) or parsed < 0:
        LOG.warning(f"Ignoring invalid {label}: {value!r}")
        return default
    return parsed


def websocket_keepalive_options(
    ping_interval=None,
    ping_timeout=None,
    interval_envs: Sequence[str] = CLIENT_PING_INTERVAL_ENVS,
    timeout_envs: Sequence[str] = CLIENT_PING_TIMEOUT_ENVS,
    disabled_interval_value=0,
) -> dict:
    """Return validated ping kwargs for websocket clients.

    A ping interval of ``0`` disables client pings. If a timeout is disabled
    or set to ``0``, the underlying library decides how long to wait for pong.
    """
    interval = _non_negative_float(
        ping_interval if ping_interval is not None else _first_env_value(interval_envs),
        DEFAULT_WEBSOCKET_PING_INTERVAL,
        "websocket ping interval",
    )
    timeout = _non_negative_float(
        ping_timeout if ping_timeout is not None else _first_env_value(timeout_envs),
        DEFAULT_WEBSOCKET_PING_TIMEOUT,
        "websocket ping timeout",
    )

    if interval <= 0:
        return {"ping_interval": disabled_interval_value}

    options = {"ping_interval": interval}
    if timeout > 0:
        if timeout >= interval:
            adjusted = max(0.1, interval / 2.0)
            LOG.warning(
                "WebSocket ping timeout must be lower than ping interval; "
                f"using {adjusted:g}s instead of {timeout:g}s"
            )
            timeout = adjusted
        options["ping_timeout"] = timeout
    else:
        options["ping_timeout"] = None
    return options
