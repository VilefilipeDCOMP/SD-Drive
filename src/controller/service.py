from __future__ import annotations

import logging

import grpc

from common.ports import MetadataPort
from controller.node_registry import (
    NODE_DRAINING,
    NODE_STATE_UNSPECIFIED,
    NodeRegistry,
)
from controller.reconciliation import InventoryItem, ReconciliationService
from controller.recovery import RecoveryManager

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
    from google.protobuf.empty_pb2 import Empty
    import dcomp_drive_pb2 as pb
    import dcomp_drive_pb2_grpc as rpc
    _PROTO_AVAILABLE = True
    _ServicerBase = rpc.ControlServiceServicer
except ImportError:
    pb = None
    rpc = None
    Empty = None
    _PROTO_AVAILABLE = False
    _ServicerBase = object


class ControlServicer(_ServicerBase):
    def __init__(
        self,
        node_registry: NodeRegistry,
        metadata_port: MetadataPort,
        recovery_manager: RecoveryManager,
    ) -> None:
        self._registry = node_registry
        self._meta = metadata_port
        self._recovery = recovery_manager
        self._reconciler = ReconciliationService(metadata_port)

    def RegisterNode(self, request, context):
        node = request.node
        try:
            config = self._registry.register_node(
                node_id=node.node_id,
                machine_id=node.machine_id,
                disk_id=node.disk_id,
                grpc_target=node.grpc_target,
                boot_id=request.boot_id,
                total_bytes=request.total_bytes,
                used_bytes=request.used_bytes,
            )
            if not _PROTO_AVAILABLE:
                return config

            return pb.RegisterNodeResponse(
                heartbeat_interval_seconds=config["heartbeat_interval_seconds"],
                suspect_timeout_seconds=config["suspect_timeout_seconds"],
                dead_timeout_seconds=config["dead_timeout_seconds"],
            )
        except Exception as exc:
            logger.error("log.record", msg=f"erro em RegisterNode: {exc}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return pb.RegisterNodeResponse() if _PROTO_AVAILABLE else {}

    def Heartbeat(self, request, context):
        recognized = self._registry.process_heartbeat(
            node_id=request.node_id,
            boot_id=request.boot_id,
            total_bytes=request.total_bytes,
            used_bytes=request.used_bytes,
            available_bytes=request.available_bytes,
        )
        if not recognized:
            current_state = self._registry.get_node_state(request.node_id)
            if current_state == NODE_STATE_UNSPECIFIED:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(
                    f"No {request.node_id!r} nao reconhecido; chame RegisterNode."
                )
            else:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    f"boot_id desatualizado para {request.node_id!r}; chame RegisterNode."
                )
            return Empty() if _PROTO_AVAILABLE else {}

        return Empty() if _PROTO_AVAILABLE else {}

    def ReportCorruption(self, request, context):
        try:
            self._recovery.handle_corruption_report(
                block_id=request.block_id,
                suspected_node_id=request.suspected_node_id,
                observed_sha256=bytes(request.observed_sha256),
                reporter_id=request.reporter_id,
            )
        except Exception as exc:
            logger.error(
                "log.record",
                msg=f"erro em ReportCorruption: {exc}",
                block_id=request.block_id,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))

        return Empty() if _PROTO_AVAILABLE else {}

    def Reconcile(self, request_iterator, context):
        node_id = None
        boot_id = None

        def _gen_batches():
            nonlocal node_id, boot_id
            for batch in request_iterator:
                if node_id is None:
                    node_id = batch.node_id
                    boot_id = batch.boot_id
                items = [
                    InventoryItem(
                        block_id=b.block_id,
                        version=b.version,
                        stored_size_bytes=b.stored_size_bytes,
                        storage_sha256=bytes(b.storage_sha256),
                    )
                    for b in batch.blocks
                ]
                yield (batch.node_id, batch.boot_id, items)

        try:
            obsolete = self._reconciler.reconcile(
                node_id=node_id or "",
                boot_id=boot_id or "",
                inventory_batches=_gen_batches(),
            )
        except Exception as exc:
            logger.error("log.record", msg=f"erro em Reconcile: {exc}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return pb.ReconcileResponse() if _PROTO_AVAILABLE else {"obsolete_block_ids": []}

        if _PROTO_AVAILABLE:
            return pb.ReconcileResponse(obsolete_block_ids=obsolete)
        return {"obsolete_block_ids": obsolete}

    def GetStatus(self, request, context):
        nodes_info = self._registry.get_all_nodes()

        healthy = under_rep = recovering = unavailable = 0
        try:
            under_blocks = self._meta.list_under_replicated_blocks()
            in_flight = self._recovery.in_flight_block_ids()
            for record in under_blocks:
                live = record.get("live_replica_count", 0)
                if live == 0:
                    unavailable += 1
                elif record.get("block_id") in in_flight:
                    recovering += 1
                else:
                    under_rep += 1
        except Exception:
            pass

        running_jobs = self._recovery.running_job_count()

        if _PROTO_AVAILABLE:
            node_statuses = [
                pb.NodeStatus(
                    node=pb.NodeAddress(
                        node_id=n.node_id,
                        machine_id=n.machine_id,
                        disk_id=n.disk_id,
                        grpc_target=n.grpc_target,
                    ),
                    state=n.state,
                    total_bytes=n.total_bytes,
                    used_bytes=n.used_bytes,
                    available_bytes=n.available_bytes,
                )
                for n in nodes_info
            ]
            return pb.GetStatusResponse(
                nodes=node_statuses,
                healthy_files=healthy,
                under_replicated_files=under_rep,
                recovering_files=recovering,
                unavailable_files=unavailable,
                running_recovery_jobs=running_jobs,
            )

        return {
            "nodes": [
                {
                    "node_id": n.node_id,
                    "machine_id": n.machine_id,
                    "disk_id": n.disk_id,
                    "grpc_target": n.grpc_target,
                    "state": n.state,
                    "total_bytes": n.total_bytes,
                    "used_bytes": n.used_bytes,
                    "available_bytes": n.available_bytes,
                }
                for n in nodes_info
            ],
            "healthy_files": healthy,
            "under_replicated_files": under_rep,
            "recovering_files": recovering,
            "unavailable_files": unavailable,
            "running_recovery_jobs": running_jobs,
        }

    def DrainNode(self, request, context):
        node_id = request.node_id

        state = self._registry.get_node_state(node_id)
        if state == NODE_STATE_UNSPECIFIED:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"No {node_id!r} nao encontrado.")
            return pb.DrainNodeResponse() if _PROTO_AVAILABLE else {"job_id": ""}

        if state == NODE_DRAINING:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"No {node_id!r} ja esta em DRAINING.")
            return pb.DrainNodeResponse() if _PROTO_AVAILABLE else {"job_id": ""}

        self._registry.set_draining(node_id)
        job_id = self._recovery.drain_node(node_id)

        if _PROTO_AVAILABLE:
            return pb.DrainNodeResponse(job_id=job_id)
        return {"job_id": job_id}
