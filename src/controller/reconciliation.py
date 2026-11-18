from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Iterator

from common.ports import MetadataPort

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
    _PROTO_AVAILABLE = True
except ImportError:
    _PROTO_AVAILABLE = False


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
                continue
            all_blocks.extend(items)

        obsolete_ids: list[str] = []

        try:
            records = self._meta.list_blocks_on_node(node_id)
        except Exception as exc:
            logger.error(
                "log.record",
                msg=f"falha ao consultar blocos do no na reconciliacao: {exc}",
                node_id=node_id,
            )
            return []

        known_on_node = {r["block_id"]: r for r in records}

        for item in all_blocks:
            record = known_on_node.get(item.block_id)
            if record is None:
                obsolete_ids.append(item.block_id)
                continue

            replica_healthy = True
            for replica in record.get("replicas", []):
                if replica.node.node_id == node_id:
                    replica_healthy = replica.healthy
                    break

            if not replica_healthy and _PROTO_AVAILABLE:
                manifest = record.get("manifest")
                if (
                    manifest is not None
                    and item.stored_size_bytes == manifest.stored_size_bytes
                    and item.storage_sha256 == manifest.storage_sha256
                ):
                    try:
                        receipt = pb.BlockReceipt(
                            block_id=item.block_id,
                            node_id=node_id,
                            stored_size_bytes=item.stored_size_bytes,
                            verified_storage_sha256=item.storage_sha256,
                        )
                        self._meta.register_recovered_replica(item.block_id, receipt)
                    except Exception:
                        pass

        logger.info(
            "reconcile.completed",
            node_id=node_id,
            reported_blocks=len(all_blocks),
            obsolete_blocks=len(obsolete_ids),
        )

        return obsolete_ids
