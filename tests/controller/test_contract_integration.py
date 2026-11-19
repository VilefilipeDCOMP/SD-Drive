from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import grpc
import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc
from google.protobuf.empty_pb2 import Empty
from controller.node_registry import NodeRegistry
from controller.recovery import RecoveryManager
from controller.service import ControlServicer
from metadata.service import MetadataService

def test_real_registry_allocates_and_resolves_committed_file():
    registry = NodeRegistry()
    for index in range(3):
        registry.register_node(str(uuid4()), f"m{index}", f"d{index}", f"n{index}:50052", str(uuid4()), 10000, 0)
    metadata = MetadataService(registry)
    block_id = str(uuid4())
    request = pb.AllocateUploadRequest(path="/integracao.txt", operation_id=str(uuid4()), plaintext_size_bytes=10, plaintext_sha256=b"a" * 32, encryption=pb.ENCRYPTION_NONE, chunks=[pb.ChunkManifest(block_id=block_id, stored_size_bytes=10, storage_sha256=b"b" * 32)])
    allocation = metadata.allocate_upload(request)
    receipts = [pb.BlockReceipt(block_id=block_id, node_id=node.node_id, stored_size_bytes=10, verified_storage_sha256=b"b" * 32) for node in allocation.chunks[0].targets]
    metadata.commit_upload(pb.CommitUploadRequest(upload_id=allocation.upload_id, receipts=receipts))
    result = metadata.resolve_download(pb.ResolveDownloadRequest(path=request.path))
    assert result.file.replication_state == pb.REPLICATION_HEALTHY
    assert len(result.chunks[0].replicas) == 3

def test_get_status_counts_each_committed_file_once_by_replication_state():
    registry = NodeRegistry()
    for index in range(3):
        registry.register_node(str(uuid4()), f"m{index}", f"d{index}", f"n{index}:50052", str(uuid4()), 10000, 0)
    metadata = MetadataService(registry)

    def upload(path, commit=True):
        chunks = [
            pb.ChunkManifest(block_id=str(uuid4()), index=index, stored_size_bytes=10, storage_sha256=b"b" * 32)
            for index in range(2)
        ]
        request = pb.AllocateUploadRequest(
            path=path, operation_id=str(uuid4()), plaintext_size_bytes=20,
            plaintext_sha256=b"a" * 32, encryption=pb.ENCRYPTION_NONE, chunks=chunks,
        )
        allocation = metadata.allocate_upload(request)
        if commit:
            receipts = [
                pb.BlockReceipt(block_id=chunk.block_id, node_id=node.node_id,
                                stored_size_bytes=10, verified_storage_sha256=b"b" * 32)
                for chunk in allocation.chunks for node in chunk.targets
            ]
            metadata.commit_upload(pb.CommitUploadRequest(upload_id=allocation.upload_id, receipts=receipts))
        return allocation.chunks

    upload("/integro")
    for chunk in upload("/sub-replicado"):
        metadata.mark_replica_invalid(chunk.block_id, chunk.targets[0].node_id)
    recovering = upload("/recuperando")[0]
    metadata.mark_replica_invalid(recovering.block_id, recovering.targets[0].node_id)
    metadata.set_recovery_state(recovering.block_id, True)
    missing, rebuilding = upload("/indisponivel")
    for node in missing.targets:
        metadata.mark_replica_invalid(missing.block_id, node.node_id)
    metadata.mark_replica_invalid(rebuilding.block_id, rebuilding.targets[0].node_id)
    metadata.set_recovery_state(rebuilding.block_id, True)
    upload("/pendente", commit=False)
    upload("/apagado")
    metadata.Delete(pb.DeleteRequest(path="/apagado"), None)

    manager = RecoveryManager(metadata, registry)
    with ThreadPoolExecutor(max_workers=1) as pool:
        server = grpc.server(pool)
        rpc.add_ControlServiceServicer_to_server(ControlServicer(registry, metadata, manager), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                status = rpc.ControlServiceStub(channel).GetStatus(Empty(), timeout=5)
        finally:
            server.stop(0).wait()
            manager.stop()

    assert status.healthy_files == 1
    assert status.under_replicated_files == 1
    assert status.recovering_files == 1
    assert status.unavailable_files == 1
    assert status.running_recovery_jobs == 0

def test_reconcile_consumes_first_batch_and_returns_obsolete_blocks():
    class Metadata:
        def list_blocks_on_node(self, node_id):
            assert node_id == "node-1"
            return []

    control = ControlServicer(NodeRegistry(), Metadata(), None)
    batch = pb.InventoryBatch(node_id="node-1", boot_id="boot-1", blocks=[pb.StoredBlockInfo(block_id="obsoleto")])
    result = control.Reconcile(iter([batch]), None)
    assert list(result.obsolete_block_ids) == ["obsoleto"]

def test_copy_returns_protobuf_receipt_with_manifest_hash():
    class Storage(rpc.StorageServiceServicer):
        def CopyBlock(self, request, context):
            assert request.expected_storage_sha256 == b"b" * 32
            assert request.version == 2
            return pb.BlockReceipt(block_id=request.block_id, node_id=request.destination.node_id, stored_size_bytes=request.stored_size_bytes, verified_storage_sha256=request.expected_storage_sha256)

    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    rpc.add_StorageServiceServicer_to_server(Storage(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    manager = RecoveryManager(None, NodeRegistry())
    try:
        receipt = manager._call_copy_block(source_grpc_target=f"127.0.0.1:{port}", block_id="bloco", destination=pb.NodeAddress(node_id="destino", grpc_target="n:50052"), stored_size_bytes=10, version=2, storage_sha256=b"b" * 32)
        assert isinstance(receipt, pb.BlockReceipt)
        assert receipt.verified_storage_sha256 == b"b" * 32
    finally:
        manager.stop()
        server.stop(0).wait()
