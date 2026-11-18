from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

DEFAULT_EVENTS_PATH = os.getenv("DCOMP_CLIENT_EVENTS_PATH", "/logs/client.jsonl")

#Registra um evento estruturado de operação concluída do cliente em formato JSON Lines.
def record_operation_event(
    operation: str,
    duration_ms: float | int,
    bytes_transferred: int = 0,
    success: bool = True,
    error_code: Optional[str] = None,
    operation_id: Optional[str] = None,
    events_path: Optional[str] = None,
) -> Dict[str, Any]:
    
    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    event: Dict[str, Any] = {
        "ts": ts,
        "component": "client",
        "event": "client.operation_completed",
        "level": "info" if success else "error",
        "operation": operation,
        "duration_ms": round(float(duration_ms), 2),
        "bytes": int(bytes_transferred),
        "success": bool(success),
    }

    if error_code is not None:
        event["error_code"] = str(error_code)
    elif not success:
        event["error_code"] = "UNKNOWN_ERROR"

    if operation_id is not None:
        event["operation_id"] = str(operation_id)

    target_path_str = events_path or DEFAULT_EVENTS_PATH
    target_path = Path(target_path_str)

    try:
        if target_path.parent and not target_path.parent.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)

        line = json.dumps(event, separators=(",", ":")) + "\n"
        with open(target_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        logger.warning(
            f"Não foi possível gravar evento de observabilidade em '{target_path}': {e}"
        )

    return event


class OperationTracker:
    #Mede a duração de uma operação e registra automaticamente o evento ao término

    def __init__(
        self,
        operation: str,
        events_path: Optional[str] = None,
        operation_id: Optional[str] = None,
    ):
        self.operation = operation
        self.events_path = events_path
        self.operation_id = operation_id
        self.bytes_transferred: int = 0
        self.start_time: float = 0.0
        self.success: bool = False
        self.error_code: Optional[str] = None

    def set_bytes(self, count: int) -> None:
        self.bytes_transferred = max(0, count)

    def set_operation_id(self, op_id: str) -> None:
        self.operation_id = op_id

    def record_error(self, code: str) -> None:
        self.error_code = code
        self.success = False

    def __enter__(self) -> OperationTracker:
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        duration_ms = (time.perf_counter() - self.start_time) * 1000.0
        if exc_type is None:
            self.success = True
        else:
            self.success = False
            if not self.error_code:
                if hasattr(exc_val, "code") and callable(exc_val.code):
                    code = exc_val.code()
                    self.error_code = code.name if hasattr(code, "name") else str(code)
                else:
                    self.error_code = exc_type.__name__

        record_operation_event(
            operation=self.operation,
            duration_ms=duration_ms,
            bytes_transferred=self.bytes_transferred,
            success=self.success,
            error_code=self.error_code,
            operation_id=self.operation_id,
            events_path=self.events_path,
        )
        return False  
