from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Optional

COMPONENT = "storage"
_lock = threading.Lock()
_node_id: Optional[str] = None


def set_node_id(node_id: Optional[str]) -> None:
    global _node_id
    _node_id = node_id


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def emit(event: str, level: str = "info", **fields: Any) -> None:
    record: dict[str, Any] = {"ts": now_iso(), "component": COMPONENT, "event": event, "level": level}
    if _node_id:
        record["node_id"] = _node_id
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (bytes, bytearray)):
            value = bytes(value).hex()
        record[key] = value
    line = json.dumps(record, separators=(",", ":"), ensure_ascii=False, default=str)
    with _lock:
        try:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
        except (OSError, ValueError):
            pass
