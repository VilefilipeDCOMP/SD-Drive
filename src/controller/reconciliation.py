from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Iterator

from src.common.logging_config import get_logger
from src.common.ports import MetadataPort, ReplicaReceipt

logger = get_logger("controller.reconciliation")


@dataclass
class InventoryItem:
    block_id: str
    version: int
    stored_size_bytes: int
    storage_sha256: bytes


class ReconciliationService:

    def __init__(self, metadata_port: MetadataPort) -> None:
        self._meta = metadata_port
        self._lock = threading.Lock()

    def reconcile(
        self,
        node_id: str,
        boot_id: str,
        inventory_batches: Iterator[tuple[str, str, list[InventoryItem]]],
    ) -> list[str]:
        all_blocks: list[InventoryItem] = []

        for batch_node_id, batch_boot_id, items in inventory_batches:
            if batch_node_id != node_id or batch_boot_id != boot_id:
                logger.warning(
                    "Lote de reconciliação com IDs inconsistentes",
                    extra={
                        "event": "reconcile.batch_id_mismatch",
                        "node_id": node_id,
                        "batch_node_id": batch_node_id,
                    },
                )
                continue
            all_blocks.extend(items)

        logger.info(
            "Reconciliando inventário do nó",
            extra={
                "event": "reconcile.started",
                "node_id": node_id,
                "boot_id": boot_id,
                "total_blocks": len(all_blocks),
            },
        )

        obsolete_ids: list[str] = []
        reused = 0

        try:
            known_on_node = {
                b.block_id: b
                for b in self._meta.list_blocks_on_node(node_id)
            }
        except Exception as exc:
            logger.error(
                "Falha ao consultar blocos do nó nos metadados",
                extra={
                    "event": "reconcile.metadata_error",
                    "node_id": node_id,
                    "error": str(exc),
                },
            )
            return []

        inventory_set = {item.block_id for item in all_blocks}

        for item in all_blocks:
            known = known_on_node.get(item.block_id)
            if known is None:
                obsolete_ids.append(item.block_id)
                continue

            if not known.healthy:
                try:
                    receipt = ReplicaReceipt(
                        block_id=item.block_id,
                        node_id=node_id,
                        stored_size_bytes=item.stored_size_bytes,
                        verified_sha256=item.storage_sha256,
                    )
                    self._meta.register_recovered_replica(item.block_id, receipt)
                    reused += 1
                    logger.debug(
                        "Réplica reaproveitada na reconciliação",
                        extra={
                            "event": "reconcile.replica_reused",
                            "node_id": node_id,
                            "block_id": item.block_id,
                        },
                    )
                except Exception as exc:
                    logger.warning(
                        "Falha ao reaproveitar réplica na reconciliação",
                        extra={
                            "event": "reconcile.reuse_failed",
                            "block_id": item.block_id,
                            "node_id": node_id,
                            "error": str(exc),
                        },
                    )

        logger.info(
            "Reconciliação concluída",
            extra={
                "event": "reconcile.completed",
                "node_id": node_id,
                "total_blocks": len(all_blocks),
                "obsolete": len(obsolete_ids),
                "reused": reused,
            },
        )

        return obsolete_ids
