"""Identity and synchronization for one isolated MCP runtime process."""

from datetime import datetime, timezone
from threading import RLock
from uuid import uuid4


# The active profile, Config class, and cached connector are process-wide state.
# Serializing MCP operations prevents a query from observing a half-finished
# profile switch or using a connector with another profile's credentials.
runtime_lock = RLock()
runtime_id = uuid4().hex[:12]
runtime_started_at = datetime.now(timezone.utc).isoformat()


def runtime_metadata() -> dict[str, str]:
    """Return non-secret identity metadata for this process-isolated session."""

    return {
        "runtime_id": runtime_id,
        "runtime_started_at": runtime_started_at,
        "session_isolation": "one_client_per_process",
    }
