"""metricas do dcomp drive: registro, endpoint http e interceptor grpc.

cada processo de longa duracao (central e nos) cria um Metrics, instala o interceptor no
servidor grpc e sobe o endpoint:

    metrics = Metrics()
    server = grpc.server(pool, interceptors=[MetricsInterceptor(metrics, "storage")])
    start_metrics_server(cfg.observability.metrics_port, metrics)

o endpoint responde em:
    /metrics        texto no formato prometheus
    /metrics.json   as mesmas series em json simples (sem os buckets dos histogramas)
    /healthz        200 se o processo esta de pe

os outros modulos atualizam as metricas de dominio direto no objeto, por exemplo:

    metrics.corruptions_detected.labels(source="scrub").inc()
    metrics.recovery_bytes_copied.inc(n_bytes)
    metrics.failure_detection_seconds.observe(segundos)

para os gauges de estado do cluster (nos por estado, arquivos por estado de replicacao) o
central registra um StatusCollector com uma funcao que devolve o status atual; o coletor so
chama essa funcao quando alguem faz scrape, entao nao custa nada fora disso.

a cli vive pouco e nao expoe /metrics: ela emite o evento client.operation_completed no log.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Iterable, Iterator, Mapping

import grpc
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.exposition import CONTENT_TYPE_LATEST

from common.logging import get_logger, utc_timestamp

RPC_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
# recuperacao e deteccao levam segundos ou minutos, entao usam buckets mais largos
LONG_BUCKETS = (0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1800)

NODE_STATE_LABELS = ("ALIVE", "SUSPECT", "DEAD", "DRAINING")
REPLICATION_STATE_LABELS = ("HEALTHY", "UNDER_REPLICATED", "RECOVERING", "UNAVAILABLE")


class Metrics:
    """conjunto de metricas de um processo, com um registry proprio (nada de estado global)."""

    def __init__(self, registry: CollectorRegistry | None = None):
        self.registry = registry or CollectorRegistry()
        reg = self.registry
        self.rpc_duration = Histogram(
            "dcomp_rpc_duration_seconds", "duracao das chamadas grpc atendidas",
            ["service", "method", "code"], buckets=RPC_BUCKETS, registry=reg,
        )
        self.rpc_bytes = Counter(
            "dcomp_rpc_bytes", "bytes protobuf trocados nas chamadas grpc",
            ["method", "direction"], registry=reg,
        )
        self.uploads = Counter("dcomp_uploads", "uploads finalizados", ["result"], registry=reg)
        self.upload_retries = Counter("dcomp_upload_retries", "novas tentativas de upload", registry=reg)
        self.corruptions_detected = Counter(
            "dcomp_corruptions_detected", "replicas corrompidas detectadas", ["source"], registry=reg,
        )
        self.scrub_blocks_checked = Counter(
            "dcomp_scrub_blocks_checked", "blocos conferidos pelo scrubbing", registry=reg,
        )
        self.recovery_bytes_copied = Counter(
            "dcomp_recovery_bytes_copied", "bytes copiados na re-replicacao", registry=reg,
        )
        self.recovery_duration = Histogram(
            "dcomp_recovery_duration_seconds", "tempo de cada copia de recuperacao",
            buckets=LONG_BUCKETS, registry=reg,
        )
        self.failure_detection_seconds = Histogram(
            "dcomp_failure_detection_seconds", "silencio ate o no ser marcado como DEAD",
            buckets=LONG_BUCKETS, registry=reg,
        )
        self.recovery_jobs_running = Gauge(
            "dcomp_recovery_jobs_running", "copias de recuperacao em andamento", registry=reg,
        )
        self.blocks_under_replicated = Gauge(
            "dcomp_blocks_under_replicated", "blocos abaixo de N, por numero de replicas vivas",
            ["live_replicas"], registry=reg,
        )
        self.node_capacity_bytes = Gauge(
            "dcomp_node_capacity_bytes", "capacidade reportada pelo proprio no",
            ["kind"], registry=reg,
        )

    def as_json(self) -> dict[str, Any]:
        """series atuais em json simples; os buckets ficam de fora para o texto nao crescer demais."""
        series = []
        for family in self.registry.collect():
            for sample in family.samples:
                if sample.name.endswith("_bucket"):
                    continue
                series.append({"name": sample.name, "labels": dict(sample.labels), "value": sample.value})
        return {"ts": utc_timestamp(), "metrics": series}


def _size(message: Any) -> int:
    byte_size = getattr(message, "ByteSize", None)
    if callable(byte_size):
        return byte_size()
    if isinstance(message, (bytes, bytearray)):
        return len(message)
    return 0


def _context_code(context: Any) -> str | None:
    # o grpc coloca o codigo no contexto antes de levantar a excecao do abort()
    try:
        code = context.code()
    except Exception:
        return None
    return getattr(code, "name", None)


class MetricsInterceptor(grpc.ServerInterceptor):
    """mede toda chamada grpc do servidor, inclusive as de streaming, e loga rpc.completed em DEBUG."""

    def __init__(self, metrics: Metrics, component: str):
        self.metrics = metrics
        self.log = get_logger(component)

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        _, _, service_and_method = handler_call_details.method.partition("/")
        service, _, method = service_and_method.partition("/")
        kwargs = {
            "request_deserializer": handler.request_deserializer,
            "response_serializer": handler.response_serializer,
        }
        if handler.unary_unary:
            return grpc.unary_unary_rpc_method_handler(self._unary_unary(handler.unary_unary, service, method), **kwargs)
        if handler.unary_stream:
            return grpc.unary_stream_rpc_method_handler(self._unary_stream(handler.unary_stream, service, method), **kwargs)
        if handler.stream_unary:
            return grpc.stream_unary_rpc_method_handler(self._stream_unary(handler.stream_unary, service, method), **kwargs)
        if handler.stream_stream:
            return grpc.stream_stream_rpc_method_handler(self._stream_stream(handler.stream_stream, service, method), **kwargs)
        return handler

    def _finish(self, service, method, start, context, error, bytes_in, bytes_out):
        duration = time.perf_counter() - start
        if error is None:
            code = _context_code(context) or "OK"
        elif isinstance(error, GeneratorExit):
            code = "CANCELLED"
        else:
            code = _context_code(context) or "UNKNOWN"
        self.metrics.rpc_duration.labels(service, method, code).observe(duration)
        if bytes_in:
            self.metrics.rpc_bytes.labels(method, "in").inc(bytes_in)
        if bytes_out:
            self.metrics.rpc_bytes.labels(method, "out").inc(bytes_out)
        self.log.debug(
            "rpc.completed", grpc_method=method, grpc_code=code, duration_ms=round(duration * 1000, 3),
            bytes_in=bytes_in, bytes_out=bytes_out,
        )

    def _unary_unary(self, fn, service, method):
        def behavior(request, context):
            start, error, out = time.perf_counter(), None, 0
            try:
                response = fn(request, context)
                out = _size(response)
                return response
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(service, method, start, context, error, _size(request), out)
        return behavior

    def _unary_stream(self, fn, service, method):
        def behavior(request, context):
            start, error, out = time.perf_counter(), None, 0
            try:
                for item in fn(request, context):
                    out += _size(item)
                    yield item
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(service, method, start, context, error, _size(request), out)
        return behavior

    def _stream_unary(self, fn, service, method):
        def behavior(request_iterator, context):
            start, error, out = time.perf_counter(), None, 0
            counted = [0]
            try:
                response = fn(_count(request_iterator, counted), context)
                out = _size(response)
                return response
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(service, method, start, context, error, counted[0], out)
        return behavior

    def _stream_stream(self, fn, service, method):
        def behavior(request_iterator, context):
            start, error, out = time.perf_counter(), None, 0
            counted = [0]
            try:
                for item in fn(_count(request_iterator, counted), context):
                    out += _size(item)
                    yield item
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._finish(service, method, start, context, error, counted[0], out)
        return behavior


def _count(iterator: Iterable[Any], total: list[int]) -> Iterator[Any]:
    for item in iterator:
        total[0] += _size(item)
        yield item


class StatusCollector:
    """gauges do estado do cluster, lidos na hora do scrape a partir de uma funcao do central.

    fetch deve devolver um dict neste formato (observability.status_cli.status_to_dict monta um
    igual a partir do GetStatusResponse):

        {"nodes": [{"node_id", "state", "total_bytes", "used_bytes", "available_bytes"}],
         "files": {"healthy", "under_replicated", "recovering", "unavailable"},
         "running_recovery_jobs": 0}
    """

    def __init__(self, fetch: Callable[[], Mapping[str, Any]]):
        self._fetch = fetch
        self._log = get_logger("metrics")

    def describe(self):
        # sem isso o registry chamaria collect() ja no registro, e o central ainda pode nao estar pronto
        return []

    def collect(self):
        ok = GaugeMetricFamily("dcomp_status_scrape_ok", "1 se o estado do cluster foi lido neste scrape")
        try:
            status = self._fetch()
        except Exception as exc:
            self._log.warning("log.record", msg=f"falha ao ler o estado do cluster para as metricas: {exc}", logger="metrics")
            ok.add_metric([], 0)
            yield ok
            return
        ok.add_metric([], 1)
        yield ok

        nodes = GaugeMetricFamily("dcomp_nodes", "nos por estado", labels=["state"])
        per_state = {state: 0 for state in NODE_STATE_LABELS}
        for node in status.get("nodes", []):
            per_state[node["state"]] = per_state.get(node["state"], 0) + 1
        for state, count in per_state.items():
            nodes.add_metric([state], count)
        yield nodes

        capacity = GaugeMetricFamily(
            "dcomp_cluster_node_bytes", "capacidade de cada no vista pelo central",
            labels=["node_id", "kind"],
        )
        for node in status.get("nodes", []):
            for kind in ("total", "used", "available"):
                capacity.add_metric([node["node_id"], kind], node[f"{kind}_bytes"])
        yield capacity

        files = GaugeMetricFamily("dcomp_files", "arquivos por estado de replicacao", labels=["replication_state"])
        counts = status.get("files", {})
        for label in REPLICATION_STATE_LABELS:
            files.add_metric([label], counts.get(label.lower(), 0))
        yield files

        jobs = GaugeMetricFamily("dcomp_cluster_recovery_jobs", "recuperacoes em andamento segundo o central")
        jobs.add_metric([], status.get("running_recovery_jobs", 0))
        yield jobs


class _Handler(BaseHTTPRequestHandler):
    metrics: Metrics

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            self._send(200, generate_latest(self.metrics.registry), CONTENT_TYPE_LATEST)
        elif path == "/metrics.json":
            body = json.dumps(self.metrics.as_json(), separators=(",", ":")).encode()
            self._send(200, body, "application/json")
        elif path == "/healthz":
            self._send(200, b"ok\n", "text/plain")
        else:
            self._send(404, b"nao encontrado\n", "text/plain")

    def _send(self, status: int, body: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # o http.server escreve em stderr por padrao; o scrape a cada segundo encheria o log
        pass


def start_metrics_server(port: int, metrics: Metrics, host: str = "0.0.0.0") -> ThreadingHTTPServer | None:
    """sobe o endpoint em uma thread daemon; port 0 desliga (devolve None).

    para pedir uma porta livre ao sistema nos testes, passe port=-1 e leia server.server_address[1].
    """
    if port == 0:
        return None
    handler = type("MetricsHandler", (_Handler,), {"metrics": metrics})
    server = ThreadingHTTPServer((host, max(port, 0)), handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="metrics-http", daemon=True).start()
    get_logger("metrics").info("metrics.server_started", port=server.server_address[1])
    return server
