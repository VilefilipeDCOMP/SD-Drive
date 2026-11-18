from __future__ import annotations

from typing import Callable, Optional

from . import jsonlog

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server
    _HAS_PROM = True
except ImportError:
    _HAS_PROM = False

_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120)


class Metrics:
    def __init__(self, usage_fn: Callable[[], "object"], enabled: bool = True):
        self.enabled = enabled and _HAS_PROM
        self._server = None
        if not self.enabled:
            return
        self.registry = CollectorRegistry()
        r = self.registry
        self.stores = Counter("dcomp_storage_store_total", "StoreBlock por resultado", ["result"], registry=r)
        self.bytes_in = Counter("dcomp_storage_bytes_received_total", "Bytes de blocos gravados", registry=r)
        self.bytes_out = Counter("dcomp_storage_bytes_sent_total", "Bytes de blocos enviados", ["op"], registry=r)
        self.reads = Counter("dcomp_storage_get_total", "GetBlock por resultado", ["result"], registry=r)
        self.copies = Counter("dcomp_storage_copy_total", "CopyBlock por resultado", ["result"], registry=r)
        self.deletes = Counter("dcomp_storage_delete_total", "DeleteBlock", registry=r)
        self.corruptions = Counter("dcomp_storage_corruption_detected_total",
                                   "Réplicas corrompidas/ausentes detectadas", ["source"], registry=r)
        self.scrub_runs = Counter("dcomp_storage_scrub_runs_total", "Passadas de scrubbing concluídas", registry=r)
        self.scrub_blocks = Counter("dcomp_storage_scrub_blocks_verified_total", "Blocos verificados pelo scrubbing",
                                    registry=r)
        self.scrub_last = Gauge("dcomp_storage_scrub_last_duration_seconds", "Duração da última passada", registry=r)
        self.store_seconds = Histogram("dcomp_storage_store_seconds", "Latência de StoreBlock", buckets=_BUCKETS,
                                       registry=r)
        self.get_seconds = Histogram("dcomp_storage_get_seconds", "Latência de GetBlock", buckets=_BUCKETS,
                                     registry=r)
        self.copy_seconds = Histogram("dcomp_storage_copy_seconds", "Latência de CopyBlock", buckets=_BUCKETS,
                                      registry=r)
        Gauge("dcomp_storage_total_bytes", "Capacidade lógica do nó", registry=r).set_function(
            lambda: usage_fn().total_bytes)
        Gauge("dcomp_storage_used_bytes", "Bytes usados (persistidos + reservados)", registry=r).set_function(
            lambda: usage_fn().used_bytes)
        Gauge("dcomp_storage_available_bytes", "Bytes disponíveis (cota lógica e disco físico)",
              registry=r).set_function(lambda: usage_fn().available_bytes)
        Gauge("dcomp_storage_blocks", "Blocos armazenados", registry=r).set_function(lambda: usage_fn().blocks)

    def start_http(self, port: int) -> None:
        if not self.enabled or port <= 0:
            return
        try:
            self._server = start_http_server(port, registry=self.registry)
            jsonlog.emit("storage.metrics_started", port=port)
        except OSError as exc:
            jsonlog.emit("storage.metrics_failed", level="warning", port=port, error=str(exc))

    def inc_store(self, result: str, nbytes: int = 0, seconds: Optional[float] = None) -> None:
        if self.enabled:
            self.stores.labels(result).inc()
            if nbytes:
                self.bytes_in.inc(nbytes)
            if seconds is not None:
                self.store_seconds.observe(seconds)

    def inc_get(self, result: str, nbytes: int = 0, seconds: Optional[float] = None) -> None:
        if self.enabled:
            self.reads.labels(result).inc()
            if nbytes:
                self.bytes_out.labels("get").inc(nbytes)
            if seconds is not None:
                self.get_seconds.observe(seconds)

    def inc_copy(self, result: str, nbytes: int = 0, seconds: Optional[float] = None) -> None:
        if self.enabled:
            self.copies.labels(result).inc()
            if nbytes:
                self.bytes_out.labels("copy").inc(nbytes)
            if seconds is not None:
                self.copy_seconds.observe(seconds)

    def inc_delete(self) -> None:
        if self.enabled:
            self.deletes.inc()

    def inc_corruption(self, source: str) -> None:
        if self.enabled:
            self.corruptions.labels(source).inc()

    def scrub_done(self, verified: int, seconds: float) -> None:
        if self.enabled:
            self.scrub_runs.inc()
            self.scrub_blocks.inc(verified)
            self.scrub_last.set(seconds)
