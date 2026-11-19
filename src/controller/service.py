from __future__ import annotations

import grpc
from itertools import chain

from controller.logging_compat import get_logger
from common.ports import MetadataPort
from controller.node_registry import NodeRegistry
from controller.recovery import RecoveryManager
from controller.reconciliation import ReconciliationService, InventoryItem

logger = get_logger("controller.service")

try:
    import dcomp_drive_pb2 as pb2
    import dcomp_drive_pb2_grpc as pb2_grpc
    _PROTO_AVAILABLE = True
except ImportError:
    _PROTO_AVAILABLE = False


class ControlServicer:

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

            return pb2.RegisterNodeResponse(
                heartbeat_interval_seconds=config["heartbeat_interval_seconds"],
                suspect_timeout_seconds=config["suspect_timeout_seconds"],
                dead_timeout_seconds=config["dead_timeout_seconds"],
            )
        except Exception as exc:
            logger.error(
                "Erro em RegisterNode",
                extra={"event": "rpc.register_node.error", "error": str(exc)},
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return pb2.RegisterNodeResponse() if _PROTO_AVAILABLE else {}

    def Heartbeat(self, request, context):
        recognized = self._registry.process_heartbeat(
            node_id=request.node_id,
            boot_id=request.boot_id,
            total_bytes=request.total_bytes,
            used_bytes=request.used_bytes,
            available_bytes=request.available_bytes,
        )
        if not recognized:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"Nó {request.node_id!r} não reconhecido; chame RegisterNode."
            )
            if _PROTO_AVAILABLE:
                from google.protobuf import empty_pb2
                return empty_pb2.Empty()
            return {}

        if _PROTO_AVAILABLE:
            from google.protobuf import empty_pb2
            return empty_pb2.Empty()
        return {}

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
                "Erro em ReportCorruption",
                extra={
                    "event": "rpc.report_corruption.error",
                    "block_id": request.block_id,
                    "error": str(exc),
                },
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))

        if _PROTO_AVAILABLE:
            from google.protobuf import empty_pb2
            return empty_pb2.Empty()
        return {}

    def Reconcile(self, request_iterator, context):
        batches = iter(request_iterator)
        first = next(batches, None)
        if first is None:
            return pb2.ReconcileResponse()
        node_id = first.node_id
        boot_id = first.boot_id

        def _gen_batches():
            for batch in chain((first,), batches):
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
            logger.error(
                "Erro em Reconcile",
                extra={"event": "rpc.reconcile.error", "error": str(exc)},
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            if _PROTO_AVAILABLE:
                return pb2.ReconcileResponse()
            return {"obsolete_block_ids": []}

        if _PROTO_AVAILABLE:
            return pb2.ReconcileResponse(obsolete_block_ids=obsolete)
        return {"obsolete_block_ids": obsolete}

    def GetStatus(self, request, context):
        nodes_info = self._registry.get_all_nodes()

        node_statuses = []
        if _PROTO_AVAILABLE:
            for n in nodes_info:
                node_statuses.append(
                    pb2.NodeStatus(
                        node=pb2.NodeAddress(
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
                )

        counts = self._meta.count_files_by_replication_state()
        summary = {
            "healthy_files": counts.get(pb2.REPLICATION_HEALTHY, 0),
            "under_replicated_files": counts.get(pb2.REPLICATION_UNDER_REPLICATED, 0),
            "recovering_files": counts.get(pb2.REPLICATION_RECOVERING, 0),
            "unavailable_files": counts.get(pb2.REPLICATION_UNAVAILABLE, 0),
            "running_recovery_jobs": self._recovery.running_job_count(),
        }

        if _PROTO_AVAILABLE:
            return pb2.GetStatusResponse(nodes=node_statuses, **summary)
        return {"nodes": [n.__dict__ for n in nodes_info], **summary}

    def DrainNode(self, request, context):
        node_id = request.node_id

        state = self._registry.get_node_state(node_id)
        if state == pb2.NODE_STATE_UNSPECIFIED:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Nó {node_id!r} não encontrado.")
            if _PROTO_AVAILABLE:
                return pb2.DrainNodeResponse()
            return {"job_id": ""}

        if state == pb2.NODE_DRAINING:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Nó {node_id!r} já está em DRAINING.")
            if _PROTO_AVAILABLE:
                return pb2.DrainNodeResponse()
            return {"job_id": ""}

        self._registry.set_draining(node_id)

        job_id = self._recovery.drain_node(node_id)

        logger.info(
            "DrainNode iniciado",
            extra={
                "event": "drain.rpc_accepted",
                "node_id": node_id,
                "job_id": job_id,
            },
        )

        if _PROTO_AVAILABLE:
            return pb2.DrainNodeResponse(job_id=job_id)
        return {"job_id": job_id}
