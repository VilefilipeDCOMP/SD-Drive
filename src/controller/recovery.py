from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Optional

import grpc

from common.ports import MetadataPort, NodeRegistryPort

try:
    from common.logging import get_logger
    logger = get_logger("central.control")
except ImportError:
    class _CompatLogger:
        def __init__(self, name: str) -> None:
            self._log = logging.getLogger(name)

        def debug(self, event: str, **fields) -> None:
            self._log.debug("%s %s", event, fields)

        def info(self, event: str, **fields) -> None:
            self._log.info("%s %s", event, fields)

        def warning(self, event: str, **fields) -> None:
            self._log.warning("%s %s", event, fields)

        def error(self, event: str, **fields) -> None:
            self._log.error("%s %s", event, fields)

    logger = _CompatLogger("central.control")

try:
    import dcomp_drive_pb2 as pb
    import dcomp_drive_pb2_grpc as rpc
    _PROTO_AVAILABLE = True
except ImportError:
    pb = None
    rpc = None
    _PROTO_AVAILABLE = False


@dataclass
class RecoveryJob:
    job_id: str
    block_id: str
    source_node_id: str
    destination_node_id: str
    destination_grpc_target: str
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    success: bool = False
    error: Optional[str] = None


class RecoveryManager:
    def __init__(
        self,
        metadata_port: MetadataPort,
        node_registry: NodeRegistryPort,
        max_concurrent_recoveries: int = 2,
        poll_interval_s: float = 5.0,
        gc_interval_s: float = 60.0,
        grpc_timeout_s: float = 120.0,
        metrics: Any = None,
    ) -> None:
        self._meta = metadata_port
        self._registry = node_registry
        self._max_concurrent = max_concurrent_recoveries
        self._poll_interval = poll_interval_s
        self._gc_interval = gc_interval_s
        self._grpc_timeout = grpc_timeout_s
        self._metrics = metrics

        self._semaphore = threading.Semaphore(max_concurrent_recoveries)
        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrent_recoveries + 2,
            thread_name_prefix="recovery",
        )
        self._active_jobs: dict[str, RecoveryJob] = {}
        self._jobs_lock = threading.Lock()
        self._in_flight_blocks: set[str] = set()
        self._had_deficit = False
        self._deficit_started_at: Optional[float] = None
        self._recovered_since_deficit = 0

        self._running = False
        self._recovery_thread: Optional[threading.Thread] = None
        self._gc_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._running = True
        self._recovery_thread = threading.Thread(
            target=self._recovery_loop, daemon=True, name="recovery-loop"
        )
        self._gc_thread = threading.Thread(
            target=self._gc_loop, daemon=True, name="gc-loop"
        )
        self._recovery_thread.start()
        self._gc_thread.start()

    def stop(self) -> None:
        self._running = False
        self._executor.shutdown(wait=False)

    def running_job_count(self) -> int:
        with self._jobs_lock:
            return sum(1 for j in self._active_jobs.values() if j.completed_at is None)

    def in_flight_block_ids(self) -> set[str]:
        with self._jobs_lock:
            return set(self._in_flight_blocks)

    def get_active_jobs(self) -> list[RecoveryJob]:
        with self._jobs_lock:
            return list(self._active_jobs.values())

    def handle_corruption_report(
        self,
        block_id: str,
        suspected_node_id: str,
        observed_sha256: bytes,
        reporter_id: str,
    ) -> None:
        logger.warning(
            "corruption.reported",
            block_id=block_id,
            suspected_node_id=suspected_node_id,
            reporter_id=reporter_id,
        )
        try:
            self._meta.mark_replica_invalid(block_id, suspected_node_id)
        except Exception as exc:
            logger.error(
                "log.record",
                msg=f"falha ao invalidar replica: {exc}",
                block_id=block_id,
                node_id=suspected_node_id,
            )
        record = self._find_block_record(block_id)
        self._executor.submit(self._recover_single_block, block_id, record)

    def drain_node(self, node_id: str) -> str:
        drain_job_id = str(uuid.uuid4())
        blocks = self._meta.list_blocks_on_node(node_id)
        logger.info(
            "drain.started",
            node_id=node_id,
            job_id=drain_job_id,
            blocks=len(blocks),
        )
        self._executor.submit(self._run_drain, node_id, drain_job_id, blocks)
        return drain_job_id

    def _run_drain(
        self,
        node_id: str,
        drain_job_id: str,
        blocks: Optional[list[dict]] = None,
    ) -> None:
        started = time.monotonic()
        try:
            if blocks is None:
                blocks = self._meta.list_blocks_on_node(node_id)
            futures: list[Future] = []
            for record in blocks:
                fut = self._executor.submit(
                    self._recover_single_block,
                    record["block_id"],
                    record,
                    {node_id},
                )
                futures.append(fut)

            for fut in futures:
                try:
                    fut.result(timeout=self._grpc_timeout * 2)
                except Exception as exc:
                    logger.error(
                        "log.record",
                        msg=f"erro durante drain de bloco: {exc}",
                        node_id=node_id,
                        job_id=drain_job_id,
                    )

            duration_ms = round((time.monotonic() - started) * 1000, 3)
            logger.info(
                "drain.completed",
                node_id=node_id,
                job_id=drain_job_id,
                duration_ms=duration_ms,
            )
        except Exception as exc:
            logger.error(
                "log.record",
                msg=f"falha no drain do no: {exc}",
                node_id=node_id,
                job_id=drain_job_id,
            )

    def _recovery_loop(self) -> None:
        while self._running:
            try:
                self._scan_and_recover()
            except Exception as exc:
                logger.error(
                    "log.record",
                    msg=f"erro no loop de recuperacao: {exc}",
                )
            time.sleep(self._poll_interval)

    def _scan_and_recover(self) -> None:
        try:
            under_replicated = self._meta.list_under_replicated_blocks()
        except Exception as exc:
            logger.error(
                "log.record",
                msg=f"falha ao listar blocos sub-replicados: {exc}",
            )
            return

        if not under_replicated:
            if self._had_deficit and self._deficit_started_at is not None:
                duration_ms = round((time.monotonic() - self._deficit_started_at) * 1000, 3)
                logger.info(
                    "recovery.cluster_healthy",
                    recovered_blocks=self._recovered_since_deficit,
                    duration_ms=duration_ms,
                )
                self._had_deficit = False
                self._deficit_started_at = None
                self._recovered_since_deficit = 0
            return

        if not self._had_deficit:
            self._had_deficit = True
            self._deficit_started_at = time.monotonic()
            self._recovered_since_deficit = 0

        with self._jobs_lock:
            in_flight = set(self._in_flight_blocks)

        for record in under_replicated:
            block_id = record["block_id"]
            if block_id in in_flight:
                continue
            if not self._semaphore.acquire(blocking=False):
                break
            self._semaphore.release()

            logger.info(
                "recovery.block_queued",
                block_id=block_id,
                live_replicas=record.get("live_replica_count", 0),
                desired_replicas=record.get("desired_replica_count", 0),
            )
            self._executor.submit(self._recover_single_block, block_id, record)

    def _find_block_record(self, block_id: str) -> Optional[dict]:
        try:
            for record in self._meta.list_under_replicated_blocks():
                if record.get("block_id") == block_id:
                    return record
        except Exception:
            pass
        return None

    def _recover_single_block(
        self,
        block_id: str,
        block_record: Optional[dict] = None,
        exclude_node_ids: Optional[set[str]] = None,
    ) -> None:
        with self._jobs_lock:
            if block_id in self._in_flight_blocks:
                return
            self._in_flight_blocks.add(block_id)

        self._semaphore.acquire()
        job_id = str(uuid.uuid4())
        recovery_marked = False

        if self._metrics is not None:
            self._metrics.recovery_jobs_running.inc()

        try:
            sources = self._meta.list_healthy_sources(block_id)
            if not sources:
                logger.warning(
                    "recovery.job_failed",
                    job_id=job_id,
                    block_id=block_id,
                    error="no_healthy_sources",
                )
                return

            source = sources[0]
            excluded = {s.node_id for s in sources} | (exclude_node_ids or set())

            destination = self._meta.select_destination(block_id, excluded)
            if destination is None:
                logger.warning(
                    "recovery.job_failed",
                    job_id=job_id,
                    block_id=block_id,
                    error="no_eligible_destination",
                )
                return

            if hasattr(self._meta, "set_recovery_state"):
                try:
                    self._meta.set_recovery_state(block_id, True)
                    recovery_marked = True
                except Exception:
                    pass

            job = RecoveryJob(
                job_id=job_id,
                block_id=block_id,
                source_node_id=source.node_id,
                destination_node_id=destination.node_id,
                destination_grpc_target=destination.grpc_target,
            )
            with self._jobs_lock:
                self._active_jobs[job_id] = job

            logger.info(
                "recovery.job_started",
                job_id=job_id,
                block_id=block_id,
                source_node_id=source.node_id,
                target_node_id=destination.node_id,
            )

            source_grpc_target = self._get_grpc_target(source.node_id) or getattr(
                source, "grpc_target", None
            )
            if not source_grpc_target:
                raise RuntimeError(f"grpc_target desconhecido para {source.node_id}")

            if block_record is None:
                block_record = self._find_block_record(block_id)

            manifest = block_record.get("manifest") if block_record else None
            stored_size_bytes = manifest.stored_size_bytes if manifest is not None else 0
            storage_sha256 = bytes(manifest.storage_sha256) if manifest is not None else b""
            version = block_record.get("version", 0) if block_record else 0

            receipt = self._call_copy_block(
                source_grpc_target=source_grpc_target,
                block_id=block_id,
                destination=destination,
                stored_size_bytes=stored_size_bytes,
                version=version,
                storage_sha256=storage_sha256,
            )

            self._meta.register_recovered_replica(block_id, receipt)

            job.completed_at = time.time()
            job.success = True
            duration_s = job.completed_at - job.started_at
            duration_ms = round(duration_s * 1000, 3)

            with self._jobs_lock:
                self._recovered_since_deficit += 1

            if self._metrics is not None:
                self._metrics.recovery_bytes_copied.inc(stored_size_bytes)
                self._metrics.recovery_duration.observe(duration_s)

            logger.info(
                "recovery.job_completed",
                job_id=job_id,
                block_id=block_id,
                target_node_id=destination.node_id,
                bytes=stored_size_bytes,
                duration_ms=duration_ms,
            )

        except Exception as exc:
            logger.error(
                "recovery.job_failed",
                job_id=job_id,
                block_id=block_id,
                error=str(exc),
            )
            with self._jobs_lock:
                if job_id in self._active_jobs:
                    self._active_jobs[job_id].completed_at = time.time()
                    self._active_jobs[job_id].success = False
                    self._active_jobs[job_id].error = str(exc)

        finally:
            if recovery_marked and hasattr(self._meta, "set_recovery_state"):
                try:
                    self._meta.set_recovery_state(block_id, False)
                except Exception:
                    pass
            with self._jobs_lock:
                self._in_flight_blocks.discard(block_id)
            if self._metrics is not None:
                self._metrics.recovery_jobs_running.dec()
            self._semaphore.release()

    def _get_grpc_target(self, node_id: str) -> Optional[str]:
        if hasattr(self._registry, "get_node_grpc_target"):
            return self._registry.get_node_grpc_target(node_id)
        for n in self._registry.list_eligible_nodes():
            if n.node_id == node_id:
                return n.grpc_target
        return None

    def _call_copy_block(
        self,
        source_grpc_target: str,
        block_id: str,
        destination: Any,
        stored_size_bytes: int,
        version: int,
        storage_sha256: bytes = b"",
    ) -> Any:
        if not _PROTO_AVAILABLE:
            return {
                "block_id": block_id,
                "node_id": destination.node_id,
                "stored_size_bytes": stored_size_bytes,
                "verified_storage_sha256": storage_sha256,
            }

        with grpc.insecure_channel(source_grpc_target) as channel:
            stub = rpc.StorageServiceStub(channel)
            dest_addr = pb.NodeAddress(
                node_id=destination.node_id,
                machine_id=getattr(destination, "machine_id", ""),
                disk_id=getattr(destination, "disk_id", ""),
                grpc_target=destination.grpc_target,
            )
            request = pb.CopyBlockRequest(
                block_id=block_id,
                destination=dest_addr,
                expected_storage_sha256=storage_sha256,
                stored_size_bytes=stored_size_bytes,
                version=version,
            )
            return stub.CopyBlock(request, timeout=self._grpc_timeout)

    def _gc_loop(self) -> None:
        while self._running:
            time.sleep(self._gc_interval)
            try:
                self._run_gc()
            except Exception as exc:
                logger.error(
                    "log.record",
                    msg=f"erro no loop de GC: {exc}",
                )

    def _run_gc(self) -> None:
        try:
            candidates = self._meta.gc_candidates()
        except Exception as exc:
            logger.error(
                "log.record",
                msg=f"falha ao obter candidatos de GC: {exc}",
            )
            return

        if not candidates:
            return

        for candidate in candidates:
            block_id = candidate["block_id"]
            node = candidate["node"]
            self._executor.submit(
                self._delete_block_on_node,
                block_id,
                node.node_id,
                node.grpc_target,
            )

    def _delete_block_on_node(
        self,
        block_id: str,
        node_id: str,
        fallback_grpc_target: Optional[str] = None,
    ) -> None:
        grpc_target = self._get_grpc_target(node_id) or fallback_grpc_target
        if not grpc_target:
            return

        deleted = False
        try:
            if not _PROTO_AVAILABLE:
                deleted = True
            else:
                with grpc.insecure_channel(grpc_target) as channel:
                    stub = rpc.StorageServiceStub(channel)
                    stub.DeleteBlock(
                        pb.DeleteBlockRequest(block_id=block_id),
                        timeout=30,
                    )
                deleted = True
                logger.info(
                    "gc.block_deleted",
                    block_id=block_id,
                    node_id=node_id,
                    reason="gc",
                )
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                deleted = True
        except Exception:
            pass

        if deleted and hasattr(self._meta, "acknowledge_gc"):
            try:
                self._meta.acknowledge_gc(block_id, node_id)
            except Exception:
                pass
