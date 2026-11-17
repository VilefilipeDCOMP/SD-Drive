import io
import json
import logging

import pytest

from common.logging import UnknownEventError, configure_logging, get_logger


def _capture(**kwargs):
    buf = io.StringIO()
    configure_logging(service_instance="teste-1", stream=buf, **kwargs)
    return buf


def _lines(buf):
    return [json.loads(line) for line in buf.getvalue().splitlines()]


def test_campos_obrigatorios_e_contexto():
    buf = _capture()
    get_logger("storage", node_id="n1").info("block.stored", block_id="b1", bytes=10, duration_ms=1.5)
    doc = _lines(buf)[0]
    assert doc["component"] == "storage" and doc["event"] == "block.stored"
    assert doc["service_instance"] == "teste-1" and doc["node_id"] == "n1"
    assert doc["ts"].endswith("Z") and doc["level"] == "INFO"
    assert "uncatalogued" not in doc


def test_segredos_e_bytes_nunca_vazam():
    buf = _capture()
    get_logger("client").info(
        "client.operation_completed", encryption_key="abc", data=b"x" * 1000, sha256=b"\x01\x02", ts="falso"
    )
    doc = _lines(buf)[0]
    assert doc["encryption_key"] == "[redacted]"
    assert doc["data"] == "<1000 bytes>" and doc["sha256"] == "0102"
    assert doc["field_ts"] == "falso" and doc["ts"] != "falso"


def test_evento_fora_do_catalogo():
    buf = _capture()
    get_logger("x").info("coisa.inventada")
    assert _lines(buf)[0]["uncatalogued"] is True
    _capture(strict_events=True)
    with pytest.raises(UnknownEventError):
        get_logger("x").info("coisa.inventada")


def test_timed_mede_e_marca_erro():
    buf = _capture()
    log = get_logger("storage")
    with log.timed("block.stored", block_id="b") as extra:
        extra["bytes"] = 5
    with pytest.raises(RuntimeError):
        with log.timed("block.stored", block_id="b"):
            raise RuntimeError("falhou")
    ok, bad = _lines(buf)
    assert ok["status"] == "ok" and ok["bytes"] == 5 and ok["duration_ms"] >= 0
    assert bad["status"] == "error" and bad["level"] == "ERROR" and "falhou" in bad["error"]


def test_registro_de_biblioteca_externa_vira_log_record():
    buf = _capture()
    logging.getLogger("grpc._cython").warning("aviso externo")
    doc = _lines(buf)[0]
    assert doc["event"] == "log.record" and doc["msg"] == "aviso externo"

