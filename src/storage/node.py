from __future__ import annotations

import socket
import threading
from concurrent import futures
from typing import Optional

import grpc

import dcomp_drive_pb2_grpc as pb_grpc

from . import jsonlog
from .config import Identity, StorageConfig, load_identity
from .controller_link import ControllerLink
from .metrics import Metrics
from .scrubber import Scrubber
from .service import StorageServicer
from .store import BlockStore


def _advertise_for(bind: str, port: int) -> str:
    host = bind.rsplit(":", 1)[0].strip("[]")
    if host in ("", "0.0.0.0", "::"):
        host = socket.gethostname()
    return f"{host}:{port}"


class StorageNode:
    def __init__(self, cfg: StorageConfig):
        self.cfg = cfg
        self.identity: Optional[Identity] = None
        self.store: Optional[BlockStore] = None
        self.link: Optional[ControllerLink] = None
        self.scrubber: Optional[Scrubber] = None
        self.metrics: Optional[Metrics] = None
        self.servicer: Optional[StorageServicer] = None
        self.server: Optional[grpc.Server] = None
        self.port = 0
        self.advertise = ""
        self._stopped = threading.Event()

    def start(self) -> "StorageNode":
        cfg = self.cfg
        self.identity = load_identity(cfg)
        jsonlog.set_node_id(self.identity.node_id)
        self.store = BlockStore(cfg.blocks_path, cfg.capacity_bytes, self.identity.node_id, fsync=cfg.fsync)
        self.metrics = Metrics(self.store.usage, enabled=cfg.metrics_port >= 0)
        self.servicer = StorageServicer(cfg, self.identity, self.store, self.metrics)
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=cfg.max_workers, thread_name_prefix="storage-rpc"))
        pb_grpc.add_StorageServiceServicer_to_server(self.servicer, self.server)
        self.port = self.server.add_insecure_port(cfg.bind)
        if self.port == 0:
            raise RuntimeError(f"não foi possível escutar em {cfg.bind}")
        self.advertise = cfg.advertise or _advertise_for(cfg.bind, self.port)

        self.link = ControllerLink(cfg, self.identity, self.advertise, self.store)

        def on_corruption(block_id: str, observed: bytes, source: str) -> None:
            self.metrics.inc_corruption(source)
            self.link.report_corruption(block_id, observed, source)

        self.store.corruption_listener = on_corruption
        self.scrubber = Scrubber(self.store, cfg.scrub_interval, cfg.scrub_max_bytes_per_sec, self.metrics)

        self.server.start()
        self.metrics.start_http(cfg.metrics_port)
        self.link.start()
        self.scrubber.start()
        usage = self.store.usage()
        jsonlog.emit("storage.node_started", grpc_target=self.advertise, bind=cfg.bind, boot_id=self.link.boot_id,
                     machine_id=self.identity.machine_id, disk_id=self.identity.disk_id,
                     total_bytes=usage.total_bytes, used_bytes=usage.used_bytes, blocks=usage.blocks,
                     scrub_interval_seconds=cfg.scrub_interval, controller=cfg.controller_target)
        return self

    def wait(self) -> None:
        self._stopped.wait()

    def stop(self, grace: float = 3.0) -> None:
        if self._stopped.is_set():
            return
        jsonlog.emit("storage.node_stopping")
        if self.scrubber:
            self.scrubber.stop()
        if self.server:
            self.server.stop(grace).wait(grace + 1)
        if self.link:
            self.link.stop()
        if self.servicer:
            self.servicer.close()
        self._stopped.set()
