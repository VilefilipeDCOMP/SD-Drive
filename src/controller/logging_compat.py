"""wrapper de logging para o módulo controller.

o código do controller (pessoa 3) usa a interface stdlib do python:
    logger.info("mensagem", extra={"event": "...", "campo": valor})

este módulo fornece get_logger() com essa interface.
"""
from __future__ import annotations

import logging
import json
import datetime


class _JsonHandler(logging.Handler):
    """formata cada registro como json lines (compatível com o esquema common/logging.py)."""

    def emit(self, record: logging.LogRecord) -> None:
        entry: dict = {
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "component": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra", None) or {}
        # suporte a logger.xxx("msg", extra={...})
        for k, v in vars(record).items():
            if k not in logging.LogRecord.__dict__ and k not in (
                "message", "asctime", "args", "msg", "levelname", "levelno",
                "pathname", "filename", "module", "funcName", "lineno",
                "exc_info", "exc_text", "stack_info", "created", "msecs",
                "relativeCreated", "thread", "threadName", "process",
                "processName", "name",
            ):
                entry[k] = v
        entry.update(extra)
        try:
            print(json.dumps(entry, default=str), flush=True)
        except Exception:
            pass


_configured = False


def get_logger(name: str) -> logging.Logger:
    """retorna um logger stdlib configurado para saída json lines."""
    global _configured
    if not _configured:
        root = logging.getLogger()
        if not root.handlers:
            root.addHandler(_JsonHandler())
        root.setLevel(logging.DEBUG)
        _configured = True
    return logging.getLogger(name)
