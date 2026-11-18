from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from typing import Optional

import grpc

from controller.logging_compat import get_logger
from common.ports import MetadataPort, NodeRegistryPort, ReplicaReceipt

logger = get_logger("controller.recovery")

try:
    import dcomp_drive_pb2 as pb2          # type: ignore
    import dcomp_drive_pb2_grpc as pb2_grpc  # type: ignore
    _PROTO_AVAILABLE = True
except ImportError:
    _PROTO_AVAILABLE = False
    logger.warning(
        "Stubs gRPC não encontrados — recovery usará modo stub",
        extra={"event": "recovery.proto_unavailable"},
    )


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
    ) -> None:
        self._meta = metadata_port
        self._registry = node_registry
        self._max_concurrent = max_concurrent_recoveries
        self._poll_interval = poll_interval_s
        self._gc_interval = gc_interval_s
        self._grpc_timeout = grpc_timeout_s

        self._semaphore = threading.Semaphore(max_concurrent_recoveries)
        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrent_recoveries + 2,
            thread_name_prefix="recovery",
        )
        self._active_jobs: dict[str, RecoveryJob] = {}
        self._jobs_lock = threading.Lock()

        self._in_flight_blocks: set[str] = set()

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
        logger.info(
            "RecoveryManager iniciado",
            extra={
                "event": "recovery.manager_started",
                "max_concurrent": self._max_concurrent,
                "poll_interval_s": self._poll_interval,
            },
        )

    def stop(self) -> None:
        self._running = False
        self._executor.shutdown(wait=False)

    def running_job_count(self) -> int:
        with self._jobs_lock:
            return sum(1 for j in self._active_jobs.values() if j.completed_at is None)

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
            "Corrupção reportada",
            extra={
                "event": "corruption.reported",
                "block_id": block_id,
                "suspected_node_id": suspected_node_id,
                "reporter_id": reporter_id,
                "observed_sha256": observed_sha256.hex() if observed_sha256 else "",
            },
        )
        try:
            self._meta.mark_replica_invalid(block_id, suspected_node_id)
        except Exception as exc:
            logger.error(
                "Falha ao marcar réplica inválida",
                extra={
                    "event": "corruption.mark_failed",
                    "block_id": block_id,
                    "node_id": suspected_node_id,
                    "error": str(exc),
                },
            )
        self._executor.submit(self._recover_single_block, block_id)

    def drain_node(self, node_id: str) -> str:
        drain_job_id = str(uuid.uuid4())
        logger.info(
            "Iniciando drain do nó",
            extra={
                "event": "drain.started",
                "node_id": node_id,
                "job_id": drain_job_id,
            },
        )
        self._executor.submit(self._run_drain, node_id, drain_job_id)
        return drain_job_id

    def _run_drain(self, node_id: str, drain_job_id: str) -> None:
        try:
            blocks = self._meta.list_blocks_on_node(node_id)
            logger.info(
                "Blocos para migrar no drain",
                extra={
                    "event": "drain.blocks_found",
                    "node_id": node_id,
                    "job_id": drain_job_id,
                    "block_count": len(blocks),
                },
            )
            futures: list[Future] = []
            for block_info in blocks:
                fut = self._executor.submit(
                    self._recover_single_block,
                    block_info.block_id,
                    exclude_node_ids={node_id},
                )
                futures.append(fut)

            errors = 0
            for fut in futures:
                try:
                    fut.result(timeout=self._grpc_timeout * 2)
                except Exception as exc:
                    errors += 1
                    logger.error(
                        "Erro durante drain de bloco",
                        extra={
                            "event": "drain.block_error",
                            "node_id": node_id,
                            "job_id": drain_job_id,
                            "error": str(exc),
                        },
                    )

            logger.info(
                "Drain concluído",
                extra={
                    "event": "drain.completed",
                    "node_id": node_id,
                    "job_id": drain_job_id,
                    "total_blocks": len(blocks),
                    "errors": errors,
                },
            )
        except Exception as exc:
            logger.error(
                "Falha no drain do nó",
                extra={
                    "event": "drain.failed",
                    "node_id": node_id,
                    "job_id": drain_job_id,
                    "error": str(exc),
                },
            )

    def _recovery_loop(self) -> None:
        while self._running:
            try:
                self._scan_and_recover()
            except Exception as exc:
                logger.error(
                    "Erro no loop de recuperação",
                    extra={"event": "recovery.loop_error", "error": str(exc)},
                )
            time.sleep(self._poll_interval)

    def _scan_and_recover(self) -> None:
        try:
            under_replicated = self._meta.list_under_replicated_blocks()
        except Exception as exc:
            logger.error(
                "Falha ao listar blocos sub-replicados",
                extra={"event": "recovery.list_failed", "error": str(exc)},
            )
            return

        if not under_replicated:
            return

        with self._jobs_lock:
            in_flight = set(self._in_flight_blocks)

        scheduled = 0
        for block_id, alive_replicas, desired_replicas in under_replicated:
            if block_id in in_flight:
                continue
            if not self._semaphore.acquire(blocking=False):
                break
            self._semaphore.release()

            self._executor.submit(self._recover_single_block, block_id)
            scheduled += 1

        if scheduled:
            logger.info(
                "Recuperações agendadas",
                extra={
                    "event": "recovery.scheduled",
                    "count": scheduled,
                    "total_under_replicated": len(under_replicated),
                },
            )

    def _recover_single_block(
        self,
        block_id: str,
        exclude_node_ids: Optional[set[str]] = None,
    ) -> None:
        with self._jobs_lock:
            if block_id in self._in_flight_blocks:
                return
            self._in_flight_blocks.add(block_id)

        self._semaphore.acquire()
        job_id = str(uuid.uuid4())

        try:
            sources = self._meta.list_healthy_sources(block_id)
            if not sources:
                logger.warning(
                    "Nenhuma fonte íntegra para bloco — UNAVAILABLE",
                    extra={
                        "event": "recovery.no_sources",
                        "block_id": block_id,
                        "job_id": job_id,
                    },
                )
                return

            source = sources[0]

            excluded = {source.node_id} | (exclude_node_ids or set())
            current_nodes = {s.node_id for s in sources}
            excluded |= current_nodes

            destination = self._meta.select_destination(block_id, excluded)
            if destination is None:
                logger.warning(
                    "Sem destino elegível para recuperação",
                    extra={
                        "event": "recovery.no_destination",
                        "block_id": block_id,
                        "job_id": job_id,
                    },
                )
                return

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
                "Recuperação iniciada",
                extra={
                    "event": "recovery.started",
                    "job_id": job_id,
                    "block_id": block_id,
                    "source_node_id": source.node_id,
                    "destination_node_id": destination.node_id,
                    "destination_grpc_target": destination.grpc_target,
                },
            )

            source_target = self._registry.get_node_capacity(source.node_id)
            source_grpc_target = self._get_grpc_target(source.node_id)
            if not source_grpc_target:
                raise RuntimeError(f"grpc_target desconhecido para {source.node_id}")

            receipt = self._call_copy_block(
                source_grpc_target=source_grpc_target,
                block_id=block_id,
                destination_node_id=destination.node_id,
                destination_grpc_target=destination.grpc_target,
                stored_size_bytes=source.stored_size_bytes,
                version=source.version,
            )

            self._meta.register_recovered_replica(block_id, receipt)

            job.completed_at = time.time()
            job.success = True

            logger.info(
                "Recuperação concluída com sucesso",
                extra={
                    "event": "recovery.completed",
                    "job_id": job_id,
                    "block_id": block_id,
                    "destination_node_id": destination.node_id,
                    "duration_ms": int((job.completed_at - job.started_at) * 1000),
                },
            )

        except Exception as exc:
            logger.error(
                "Recuperação falhou",
                extra={
                    "event": "recovery.failed",
                    "job_id": job_id,
                    "block_id": block_id,
                    "error": str(exc),
                },
            )
            with self._jobs_lock:
                if job_id in self._active_jobs:
                    self._active_jobs[job_id].completed_at = time.time()
                    self._active_jobs[job_id].success = False
                    self._active_jobs[job_id].error = str(exc)

        finally:
            with self._jobs_lock:
                self._in_flight_blocks.discard(block_id)
            self._semaphore.release()

    def _get_grpc_target(self, node_id: str) -> Optional[str]:
        if hasattr(self._registry, "get_node_grpc_target"):
            return self._registry.get_node_grpc_target(node_id)
        all_nodes = self._registry.list_eligible_nodes()
        for n in all_nodes:
            if n.node_id == node_id:
                return n.grpc_target
        return None

    def _call_copy_block(
        self,
        source_grpc_target: str,
        block_id: str,
        destination_node_id: str,
        destination_grpc_target: str,
        stored_size_bytes: int,
        version: int,
        storage_sha256: bytes = b"",
    ) -> ReplicaReceipt:
        if not _PROTO_AVAILABLE:
            logger.warning(
                "Modo stub: simulando CopyBlock",
                extra={
                    "event": "recovery.stub_copy",
                    "block_id": block_id,
                    "destination": destination_grpc_target,
                },
            )
            return ReplicaReceipt(
                block_id=block_id,
                node_id=destination_node_id,
                stored_size_bytes=stored_size_bytes,
                verified_sha256=storage_sha256,
            )

        with grpc.insecure_channel(source_grpc_target) as channel:
            stub = pb2_grpc.StorageServiceStub(channel)
            dest_node_addr = pb2.NodeAddress(
                node_id=destination_node_id,
                grpc_target=destination_grpc_target,
            )
            request = pb2.CopyBlockRequest(
                block_id=block_id,
                destination=dest_node_addr,
                expected_storage_sha256=storage_sha256,
                stored_size_bytes=stored_size_bytes,
                version=version,
            )
            response = stub.CopyBlock(
                request, timeout=self._grpc_timeout
            )
            return ReplicaReceipt(
                block_id=response.block_id,
                node_id=response.node_id,
                stored_size_bytes=response.stored_size_bytes,
                verified_sha256=bytes(response.verified_storage_sha256),
            )

    def _gc_loop(self) -> None:
        while self._running:
            time.sleep(self._gc_interval)
            try:
                self._run_gc()
            except Exception as exc:
                logger.error(
                    "Erro no loop de GC",
                    extra={"event": "gc.loop_error", "error": str(exc)},
                )

    def _run_gc(self) -> None:
        try:
            candidates = self._meta.gc_candidates()
        except Exception as exc:
            logger.error(
                "Falha ao obter candidatos de GC",
                extra={"event": "gc.list_failed", "error": str(exc)},
            )
            return

        if not candidates:
            return

        logger.info(
            "GC: blocos candidatos para remoção",
            extra={"event": "gc.started", "count": len(candidates)},
        )

        for block_id, node_id in candidates:
            self._executor.submit(self._delete_block_on_node, block_id, node_id)

    def _delete_block_on_node(self, block_id: str, node_id: str) -> None:
        grpc_target = self._get_grpc_target(node_id)
        if not grpc_target:
            logger.debug(
                "GC: nó sem grpc_target acessível, pulando",
                extra={"event": "gc.node_unreachable", "block_id": block_id, "node_id": node_id},
            )
            return

        try:
            if not _PROTO_AVAILABLE:
                logger.debug(
                    "GC stub: simulando DeleteBlock",
                    extra={"event": "gc.stub_delete", "block_id": block_id, "node_id": node_id},
                )
                return

            with grpc.insecure_channel(grpc_target) as channel:
                stub = pb2_grpc.StorageServiceStub(channel)
                stub.DeleteBlock(
                    pb2.DeleteBlockRequest(block_id=block_id),
                    timeout=30,
                )
            logger.info(
                "GC: bloco removido do nó",
                extra={"event": "gc.block_deleted", "block_id": block_id, "node_id": node_id},
            )
        except grpc.RpcError as exc:
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                logger.debug(
                    "GC: bloco já ausente no nó",
                    extra={"event": "gc.block_already_absent", "block_id": block_id, "node_id": node_id},
                )
            else:
                logger.warning(
                    "GC: falha ao remover bloco",
                    extra={
                        "event": "gc.delete_failed",
                        "block_id": block_id,
                        "node_id": node_id,
                        "grpc_code": str(exc.code()),
                    },
                )
        except Exception as exc:
            logger.warning(
                "GC: erro inesperado ao remover bloco",
                extra={
                    "event": "gc.delete_error",
                    "block_id": block_id,
                    "node_id": node_id,
                    "error": str(exc),
                },
            )
