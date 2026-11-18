from __future__ import annotations

import threading
import time
import uuid
from typing import Iterator, Optional

import grpc

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as pb_grpc

from . import jsonlog
from .config import Identity, StorageConfig
from .store import BlockStore

INVENTORY_BATCH_SIZE = 1000


class ControllerLink:
    def __init__(self, cfg: StorageConfig, identity: Identity, advertise: str, store: BlockStore):
        self.cfg = cfg
        self.identity = identity
        self.advertise = advertise
        self.store = store
        self.boot_id = str(uuid.uuid4())
        self._channel = grpc.insecure_channel(cfg.controller_target)
        self._stub = pb_grpc.ControlServiceStub(self._channel)
        self._halt = threading.Event()
        self._registered = False
        self._failures = 0
        self.heartbeat_interval = cfg.default_heartbeat_interval
        self._pending: dict[str, bytes] = {}
        self._cond = threading.Condition()
        self._threads = [
            threading.Thread(target=self._main_loop, name="storage-controller-link", daemon=True),
            threading.Thread(target=self._report_loop, name="storage-corruption-reporter", daemon=True),
        ]

    def start(self) -> None:
        for block_id in self.store.corrupt_block_ids():
            self.report_corruption(block_id, b"", "startup")
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._halt.set()
        with self._cond:
            self._cond.notify_all()
        for t in self._threads:
            t.join(timeout=3)
        self._channel.close()

    @property
    def registered(self) -> bool:
        return self._registered

    def _node_address(self) -> "pb.NodeAddress":
        return pb.NodeAddress(node_id=self.identity.node_id, machine_id=self.identity.machine_id,
                              disk_id=self.identity.disk_id, grpc_target=self.advertise)

    def register(self) -> None:
        usage = self.store.usage()
        resp = self._stub.RegisterNode(
            pb.RegisterNodeRequest(node=self._node_address(), boot_id=self.boot_id,
                                   total_bytes=usage.total_bytes, used_bytes=usage.used_bytes),
            timeout=self.cfg.controller_deadline)
        if resp.heartbeat_interval_seconds > 0:
            self.heartbeat_interval = float(resp.heartbeat_interval_seconds)
        self._registered = True
        self._failures = 0
        jsonlog.emit("storage.registered", boot_id=self.boot_id, grpc_target=self.advertise,
                     machine_id=self.identity.machine_id, disk_id=self.identity.disk_id,
                     heartbeat_interval_seconds=self.heartbeat_interval)

    def heartbeat(self) -> None:
        usage = self.store.usage()
        self._stub.Heartbeat(
            pb.HeartbeatRequest(node_id=self.identity.node_id, boot_id=self.boot_id,
                                total_bytes=usage.total_bytes, used_bytes=usage.used_bytes,
                                available_bytes=usage.available_bytes),
            timeout=self.cfg.controller_deadline)

    def _inventory_batches(self) -> Iterator["pb.InventoryBatch"]:
        records = self.store.inventory()
        if not records:
            yield pb.InventoryBatch(node_id=self.identity.node_id, boot_id=self.boot_id)
            return
        for i in range(0, len(records), INVENTORY_BATCH_SIZE):
            yield pb.InventoryBatch(
                node_id=self.identity.node_id, boot_id=self.boot_id,
                blocks=[pb.StoredBlockInfo(block_id=r.block_id, version=r.version, stored_size_bytes=r.size,
                                           storage_sha256=r.sha256)
                        for r in records[i:i + INVENTORY_BATCH_SIZE]])

    def reconcile(self) -> None:
        started = time.monotonic()
        resp = self._stub.Reconcile(self._inventory_batches(), timeout=max(self.cfg.controller_deadline, 60.0))
        obsolete = list(resp.obsolete_block_ids)
        removed = 0
        if self.cfg.delete_obsolete:
            for block_id in obsolete:
                try:
                    if self.store.delete(block_id):
                        removed += 1
                        jsonlog.emit("storage.obsolete_block_deleted", block_id=block_id)
                except Exception as exc:
                    jsonlog.emit("storage.obsolete_delete_failed", level="warning", block_id=block_id,
                                 error=str(exc))
        jsonlog.emit("storage.reconcile_completed", blocks_reported=len(self.store.inventory()),
                     obsolete_received=len(obsolete), obsolete_deleted=removed,
                     duration_ms=int((time.monotonic() - started) * 1000))

    def _main_loop(self) -> None:
        backoff = 0.5
        while not self._halt.is_set():
            if not self._registered:
                try:
                    self.register()
                    self.reconcile()
                    backoff = 0.5
                except grpc.RpcError as exc:
                    self._registered = False
                    jsonlog.emit("storage.controller_unavailable", level="warning", op="register",
                                 code=exc.code().name, detail=exc.details())
                    self._halt.wait(backoff)
                    backoff = min(backoff * 2, 5.0)
                continue
            self._halt.wait(self.heartbeat_interval)
            if self._halt.is_set():
                break
            try:
                self.heartbeat()
                self._failures = 0
            except grpc.RpcError as exc:
                self._failures += 1
                code = exc.code()
                jsonlog.emit("storage.heartbeat_failed", level="warning", code=code.name, detail=exc.details(),
                             consecutive_failures=self._failures)
                if code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION,
                            grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.UNAUTHENTICATED) or self._failures >= 3:
                    self._registered = False

    def report_corruption(self, block_id: str, observed: bytes, source: str = "") -> None:
        with self._cond:
            self._pending[block_id] = observed
            self._cond.notify()

    def pending_reports(self) -> int:
        with self._cond:
            return len(self._pending)

    def _report_loop(self) -> None:
        while not self._halt.is_set():
            with self._cond:
                while not self._pending and not self._halt.is_set():
                    self._cond.wait(timeout=1.0)
                if self._halt.is_set():
                    break
                batch = list(self._pending.items())
            failed = False
            for block_id, observed in batch:
                if self._halt.is_set():
                    break
                try:
                    self._stub.ReportCorruption(
                        pb.ReportCorruptionRequest(block_id=block_id, suspected_node_id=self.identity.node_id,
                                                   observed_sha256=observed, reporter_id=self.identity.node_id),
                        timeout=self.cfg.controller_deadline)
                except grpc.RpcError as exc:
                    failed = True
                    jsonlog.emit("storage.corruption_report_failed", level="warning", block_id=block_id,
                                 code=exc.code().name, detail=exc.details())
                    continue
                with self._cond:
                    if self._pending.get(block_id) == observed:
                        del self._pending[block_id]
                jsonlog.emit("storage.corruption_reported", block_id=block_id, observed_sha256=observed or None)
                if self.cfg.discard_corrupt and self.store.discard_if_corrupt(block_id):
                    jsonlog.emit("storage.corrupt_block_discarded", block_id=block_id)
            if failed:
                self._halt.wait(2.0)
