"""logs estruturados em json lines para todos os componentes (rf12).

cada linha e um objeto json com pelo menos ts, level, component, event e service_instance,
mais os ids que fizerem sentido (file_id, block_id, upload_id, node_id...). o esquema completo
e o catalogo de eventos ficam em docs/OBSERVABILIDADE.md e em common/events.py.

uso basico:

    from common.logging import configure_logging, get_logger

    configure_logging(level="INFO")                      # uma vez, no inicio do processo
    log = get_logger("storage", node_id=node_id)         # contexto fixo vai em toda linha
    log.info("block.stored", block_id=bid, bytes=n, duration_ms=12.5)

    with log.timed("block.copy_completed", block_id=bid) as extra:
        extra["bytes"] = copiar()                        # duration_ms e status entram sozinhos

cuidados que a biblioteca ja toma:
- chaves com cara de segredo (key, secret, password, token...) viram "[redacted]".
- bytes curtos (hashes, nonces) viram hex; bytes longos viram so o tamanho, nunca o conteudo.
- campos que colidem com os reservados ganham prefixo field_ em vez de sobrescrever.
- evento fora do catalogo sai marcado com uncatalogued=true (ou erro, com strict_events).
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import enum
import json
import logging
import math
import os
import socket
import sys
import threading
import time
import uuid
from pathlib import Path, PurePath
from typing import IO, Any, Iterator, Mapping

from common import events as _events

RESERVED_FIELDS = ("ts", "level", "component", "event", "service_instance", "msg")
SENSITIVE_KEY_PARTS = ("key", "secret", "password", "passwd", "token", "credential")
REDACTED = "[redacted]"
MAX_INLINE_BYTES = 64
MAX_STRING_CHARS = 8000
MAX_DEPTH = 6
INSTANCE_ENV = "DCOMP_INSTANCE"

_LOGGER_NAMESPACE = "dcomp"
_RECORD_ATTR = "dcomp_payload"


class UnknownEventError(ValueError):
    """evento fora do catalogo com strict_events ligado."""


class _State:
    # configuracao global do processo; protegida por lock porque varios threads podem logar no boot
    lock = threading.Lock()
    configured = False
    strict_events = False
    service_instance = ""
    handlers: list[logging.Handler] = []


def default_service_instance() -> str:
    """nome da instancia: DCOMP_INSTANCE se existir, senao o hostname (no compose = nome do servico)."""
    return os.environ.get(INSTANCE_ENV) or socket.gethostname()


def utc_timestamp(epoch_seconds: float | None = None) -> str:
    """iso-8601 em utc com milissegundos, ex: 2026-11-16T14:03:22.418Z."""
    moment = _dt.datetime.fromtimestamp(time.time() if epoch_seconds is None else epoch_seconds, tz=_dt.timezone.utc)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def configure_logging(
    *,
    level: str | int = "INFO",
    fmt: str = "json",
    log_file: str = "",
    service_instance: str | None = None,
    strict_events: bool = False,
    stream: IO[str] | None = None,
) -> None:
    """instala os handlers no logger raiz. pode ser chamado de novo (ex: depois de ler a config).

    stream: para onde vai a saida principal; stdout por padrao. a cli usa stderr para nao
    misturar log com a saida que o usuario le.
    log_file: arquivo extra em json lines; aceita {instance} no caminho. se nao der para abrir,
    o processo segue so com o stream e avisa no proprio log.
    """
    instance = service_instance or default_service_instance()
    if fmt not in ("json", "console"):
        raise ValueError(f"formato de log desconhecido: {fmt!r} (use json ou console)")
    formatter: logging.Formatter = JsonFormatter(instance) if fmt == "json" else ConsoleFormatter(instance)

    new_handlers: list[logging.Handler] = []
    main_handler = logging.StreamHandler(stream or sys.stdout)
    main_handler.setFormatter(formatter)
    new_handlers.append(main_handler)

    file_problem = None
    if log_file:
        path = Path(log_file.replace("{instance}", instance))
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(path, encoding="utf-8")
            # o arquivo e sempre json, mesmo com console na tela, porque e ele que vira evidencia
            file_handler.setFormatter(JsonFormatter(instance))
            new_handlers.append(file_handler)
        except OSError as exc:
            file_problem = f"nao foi possivel abrir {path}: {exc}"

    root = logging.getLogger()
    with _State.lock:
        for old in _State.handlers:
            root.removeHandler(old)
            old.close()
        for handler in new_handlers:
            root.addHandler(handler)
        _State.handlers = new_handlers
        root.setLevel(_level_number(level))
        _State.service_instance = instance
        _State.strict_events = strict_events
        _State.configured = True

    # o grpc em python loga bastante em DEBUG; deixamos so avisos dele para nao poluir
    logging.getLogger("grpc").setLevel(max(logging.WARNING, _level_number(level)))

    if file_problem:
        get_logger("logging").warning("log.record", msg=file_problem, logger="common.logging")


def configure_from_config(obs_cfg: Any, *, stream: IO[str] | None = None, service_instance: str | None = None) -> None:
    """atalho para a secao observability da config (common.config.ObservabilityConfig)."""
    configure_logging(
        level=obs_cfg.log_level,
        fmt=obs_cfg.log_format,
        log_file=obs_cfg.log_file,
        strict_events=obs_cfg.strict_events,
        service_instance=service_instance,
        stream=stream,
    )


def get_logger(component: str, **context: Any) -> EventLogger:
    """logger de eventos para um componente (central.metadata, central.control, storage, client...).

    os campos de context aparecem em todas as linhas desse logger; use bind() para acrescentar mais.
    """
    return EventLogger(component, context)


class EventLogger:
    def __init__(self, component: str, context: Mapping[str, Any] | None = None):
        if not component:
            raise ValueError("component nao pode ser vazio")
        self.component = component
        self._context = dict(context or {})
        self._logger = logging.getLogger(f"{_LOGGER_NAMESPACE}.{component}")

    @property
    def context(self) -> dict[str, Any]:
        return dict(self._context)

    def bind(self, **context: Any) -> EventLogger:
        """novo logger com o contexto atual somado ao informado (o original nao muda)."""
        return EventLogger(self.component, {**self._context, **context})

    def event(self, event: str, *, level: str | int = "INFO", msg: str | None = None, exc_info: Any = None, **fields: Any) -> None:
        self._emit(event, level, msg, exc_info, fields)

    def debug(self, event: str, *, msg: str | None = None, **fields: Any) -> None:
        self._emit(event, logging.DEBUG, msg, None, fields)

    def info(self, event: str, *, msg: str | None = None, **fields: Any) -> None:
        self._emit(event, logging.INFO, msg, None, fields)

    def warning(self, event: str, *, msg: str | None = None, **fields: Any) -> None:
        self._emit(event, logging.WARNING, msg, None, fields)

    def error(self, event: str, *, msg: str | None = None, **fields: Any) -> None:
        self._emit(event, logging.ERROR, msg, None, fields)

    def exception(self, event: str, *, msg: str | None = None, **fields: Any) -> None:
        """igual a error, mas anexa o traceback da excecao corrente. chamar dentro de um except."""
        self._emit(event, logging.ERROR, msg, True, fields)

    @contextlib.contextmanager
    def timed(self, event: str, *, level: str | int = "INFO", **fields: Any) -> Iterator[dict[str, Any]]:
        """mede o bloco e emite o evento no final com duration_ms e status (ok ou error).

        o dict devolvido pode receber campos descobertos durante o bloco (ex: bytes).
        se o bloco levantar excecao, o evento sai como ERROR com o campo error e a excecao segue.
        """
        extra: dict[str, Any] = {}
        start = time.perf_counter()
        try:
            yield extra
        except BaseException as exc:
            elapsed = (time.perf_counter() - start) * 1000.0
            payload = {**fields, **extra, "status": "error", "duration_ms": round(elapsed, 3), "error": describe_error(exc)}
            self._emit(event, logging.ERROR, None, None, payload)
            raise
        elapsed = (time.perf_counter() - start) * 1000.0
        payload = {**fields, **extra, "status": "ok", "duration_ms": round(elapsed, 3)}
        self._emit(event, level, None, None, payload)

    def _emit(self, event: str, level: str | int, msg: str | None, exc_info: Any, fields: Mapping[str, Any]) -> None:
        if not _State.configured:
            # quem esqueceu de configurar ainda ganha json no stdout em vez de log perdido
            configure_logging()
        levelno = _level_number(level)
        if not self._logger.isEnabledFor(levelno):
            return

        merged: dict[str, Any] = {}
        for key, value in {**self._context, **fields}.items():
            merged[_safe_key(key)] = REDACTED if _is_sensitive(key) else _jsonable(value, 0)
        if not _events.is_known(event):
            if _State.strict_events:
                raise UnknownEventError(f"evento '{event}' nao esta em common/events.py")
            merged["uncatalogued"] = True
        if msg is not None:
            merged["msg"] = _truncate(str(msg))

        payload = {"component": self.component, "event": event, "fields": merged}
        self._logger.log(levelno, event, exc_info=exc_info, extra={_RECORD_ATTR: payload}, stacklevel=3)


class JsonFormatter(logging.Formatter):
    """uma linha json por registro, com os campos reservados sempre na frente."""

    def __init__(self, service_instance: str):
        super().__init__()
        self.service_instance = service_instance

    def build(self, record: logging.LogRecord) -> dict[str, Any]:
        payload = getattr(record, _RECORD_ATTR, None)
        doc: dict[str, Any] = {"ts": utc_timestamp(record.created), "level": record.levelname}
        if payload is None:
            # registro de biblioteca externa (grpc, asyncio...) que nao passou pelo EventLogger
            doc.update(
                component=record.name.split(".")[0],
                event="log.record",
                service_instance=self.service_instance,
                logger=record.name,
                msg=_truncate(record.getMessage()),
            )
        else:
            doc.update(component=payload["component"], event=payload["event"], service_instance=self.service_instance)
            doc.update(payload["fields"])
        if record.exc_info:
            doc["traceback"] = _truncate(self.formatException(record.exc_info))
        return doc

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(self.build(record), ensure_ascii=False, separators=(",", ":"), default=str)


class ConsoleFormatter(JsonFormatter):
    """formato curto para desenvolvimento: hora nivel componente evento chave=valor."""

    def format(self, record: logging.LogRecord) -> str:
        doc = self.build(record)
        head = f"{doc.pop('ts')[11:23]} {doc.pop('level'):<7} {doc.pop('component')} {doc.pop('event')}"
        doc.pop("service_instance", None)
        traceback_text = doc.pop("traceback", None)
        tail = " ".join(f"{k}={json.dumps(v, ensure_ascii=False, default=str)}" for k, v in doc.items())
        line = f"{head} {tail}".rstrip()
        return f"{line}\n{traceback_text}" if traceback_text else line


def describe_error(exc: BaseException) -> str:
    """texto curto para o campo error; para grpc.RpcError inclui o codigo e o detalhe."""
    code = getattr(exc, "code", None)
    details = getattr(exc, "details", None)
    if callable(code) and callable(details):
        try:
            status = code()
            return f"{getattr(status, 'name', status)}: {details()}"
        except Exception:
            pass
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _level_number(level: str | int) -> int:
    if isinstance(level, int):
        return level
    number = logging.getLevelName(str(level).upper())
    if not isinstance(number, int):
        raise ValueError(f"nivel de log desconhecido: {level!r}")
    return number


def _is_sensitive(key: str) -> bool:
    lowered = str(key).lower()
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def _safe_key(key: Any) -> str:
    text = str(key)
    return f"field_{text}" if text in RESERVED_FIELDS and text != "msg" else text


def _truncate(text: str) -> str:
    if len(text) <= MAX_STRING_CHARS:
        return text
    return f"{text[:MAX_STRING_CHARS]}...(+{len(text) - MAX_STRING_CHARS} chars)"


def _jsonable(value: Any, depth: int) -> Any:
    # converte para tipos que o json entende sem nunca despejar o conteudo de blocos no log
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        return _truncate(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return raw.hex() if len(raw) <= MAX_INLINE_BYTES else f"<{len(raw)} bytes>"
    if isinstance(value, enum.Enum):
        return value.name
    if isinstance(value, (uuid.UUID, PurePath)):
        return str(value)
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, BaseException):
        return describe_error(value)
    if depth >= MAX_DEPTH:
        return _truncate(repr(value))
    if isinstance(value, Mapping):
        return {
            _safe_key(k): REDACTED if _is_sensitive(k) else _jsonable(v, depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        items = sorted(value, key=repr) if isinstance(value, (set, frozenset)) else value
        return [_jsonable(item, depth + 1) for item in items]
    if hasattr(value, "DESCRIPTOR") and hasattr(value, "ListFields"):
        # mensagem protobuf: so o nome do tipo, para nao vazar frames de dados por acidente
        return f"<{type(value).__name__}>"
    return _truncate(repr(value))

