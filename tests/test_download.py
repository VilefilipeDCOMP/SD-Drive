from dataclasses import replace
from uuid import uuid4

import grpc
import pytest

import dcomp_drive_pb2 as pb
from common.ports import NodeSnapshot
from common.simulated_registry import SimulatedNodeRegistry
from metadata.service import AllocationError, MetadataService

def prepare_file(*, chunk_count=2, write_quorum="all", committed=True, encrypted=False):
    nodes = []
    for index in range(3):
        nodes.append(NodeSnapshot(str(uuid4()), f"machine-{index}", f"disk-{index}", f"storage-{index}:50052", pb.NODE_ALIVE, 10000, 0, 10000))

    service = MetadataService(SimulatedNodeRegistry(nodes), write_quorum=write_quorum)
    encryption = pb.ENCRYPTION_NONE
    stored_size = 100
    if encrypted:
        encryption = pb.ENCRYPTION_AES_256_GCM
        stored_size = 116

    request = pb.AllocateUploadRequest(path="/teste.txt", operation_id=str(uuid4()), plaintext_size_bytes=100 * chunk_count, plaintext_sha256=b"a" * 32, encryption=encryption)
    for index in range(chunk_count):
        nonce = b""
        if encrypted:
            nonce = bytes([index + 1]) * 12
        request.chunks.append(pb.ChunkManifest(block_id=str(uuid4()), index=index, stored_size_bytes=stored_size, storage_sha256=b"b" * 32, nonce=nonce))

    allocation = service.allocate_upload(request)
    if committed:
        commit = pb.CommitUploadRequest(upload_id=allocation.upload_id)
        for chunk, placement in zip(request.chunks, allocation.chunks):
            for node in placement.targets[:allocation.write_quorum]:
                commit.receipts.append(pb.BlockReceipt(block_id=chunk.block_id, node_id=node.node_id, stored_size_bytes=chunk.stored_size_bytes, verified_storage_sha256=chunk.storage_sha256))
        service.commit_upload(commit)

    return service, request, allocation

def test_download_returns_committed_metadata_ordered_chunks_and_replica_addresses():
    service, request, allocation = prepare_file()

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.file_id == allocation.file_id
    assert result.file.path == "/teste.txt"
    assert result.file.version == 1
    assert result.file.state == pb.FILE_COMMITTED
    assert result.file.replication_state == pb.REPLICATION_HEALTHY
    assert result.file.plaintext_size_bytes == 200
    assert result.file.plaintext_sha256 == b"a" * 32
    assert [chunk.manifest.index for chunk in result.chunks] == [0, 1]
    for index, chunk in enumerate(result.chunks):
        assert chunk.manifest == request.chunks[index]
        assert [replica.node for replica in chunk.replicas] == list(allocation.chunks[index].targets)
        assert all(replica.healthy for replica in chunk.replicas)

@pytest.mark.parametrize("path", ["/inexistente.txt", "/teste.txt"])
def test_missing_and_pending_files_are_not_visible(path):
    service, _, _ = prepare_file(committed=False)

    with pytest.raises(AllocationError) as exc:
        service.resolve_download(pb.ResolveDownloadRequest(path=path))
    assert exc.value.code == grpc.StatusCode.NOT_FOUND

def test_failed_commit_does_not_make_file_visible():
    service, request, allocation = prepare_file(committed=False)
    with pytest.raises(AllocationError):
        service.commit_upload(pb.CommitUploadRequest(upload_id=allocation.upload_id))

    with pytest.raises(AllocationError) as exc:
        service.resolve_download(pb.ResolveDownloadRequest(path=request.path))
    assert exc.value.code == grpc.StatusCode.NOT_FOUND

@pytest.mark.parametrize("state", [pb.FILE_DELETED, pb.FILE_PENDING])
def test_only_committed_file_state_is_returned(state):
    service, request, _ = prepare_file()
    metadata = pb.FileMetadata.FromString(service._files[request.path])
    metadata.state = state
    service._files[request.path] = metadata.SerializeToString()

    with pytest.raises(AllocationError) as exc:
        service.resolve_download(pb.ResolveDownloadRequest(path=request.path))
    assert exc.value.code == grpc.StatusCode.NOT_FOUND

def test_download_does_not_return_planned_replicas_without_receipts():
    service, request, allocation = prepare_file(write_quorum=2)

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.replication_state == pb.REPLICATION_UNDER_REPLICATED
    for index, chunk in enumerate(result.chunks):
        assert [replica.node.node_id for replica in chunk.replicas] == [node.node_id for node in allocation.chunks[index].targets[:2]]

@pytest.mark.parametrize("state", [pb.NODE_DEAD, pb.NODE_SUSPECT])
def test_download_filters_nodes_that_became_unavailable_after_commit(state):
    service, request, allocation = prepare_file()
    node_id = allocation.chunks[0].targets[0].node_id
    registry = service._registry
    registry._nodes[node_id] = replace(registry._nodes[node_id], state=state)

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.replication_state == pb.REPLICATION_UNDER_REPLICATED
    for chunk in result.chunks:
        assert len(chunk.replicas) == 2
        assert node_id not in [replica.node.node_id for replica in chunk.replicas]

def test_draining_node_remains_available_for_reading():
    service, request, allocation = prepare_file()
    node_id = allocation.chunks[0].targets[0].node_id
    registry = service._registry
    registry._nodes[node_id] = replace(registry._nodes[node_id], state=pb.NODE_DRAINING)

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.replication_state == pb.REPLICATION_HEALTHY
    assert len(result.chunks[0].replicas) == 3

def test_download_reports_unavailable_file_without_hiding_its_metadata():
    service, request, _ = prepare_file()
    registry = service._registry
    stored = service._files[request.path]
    registry._nodes.clear()

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.state == pb.FILE_COMMITTED
    assert result.file.replication_state == pb.REPLICATION_UNAVAILABLE
    assert len(result.chunks) == 2
    assert all(len(chunk.replicas) == 0 for chunk in result.chunks)
    assert service._files[request.path] == stored

def test_download_uses_current_registered_address():
    service, request, allocation = prepare_file()
    node_id = allocation.chunks[0].targets[0].node_id
    registry = service._registry
    registry._nodes[node_id] = replace(registry._nodes[node_id], grpc_target="storage-new:50052")

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.chunks[0].replicas[0].node.grpc_target == "storage-new:50052"

def test_one_unavailable_chunk_makes_entire_file_unavailable():
    service, request, allocation = prepare_file(write_quorum=1, committed=False)
    commit = pb.CommitUploadRequest(upload_id=allocation.upload_id)
    for index, chunk in enumerate(request.chunks):
        target = allocation.chunks[index].targets[index]
        commit.receipts.append(pb.BlockReceipt(block_id=chunk.block_id, node_id=target.node_id, stored_size_bytes=chunk.stored_size_bytes, verified_storage_sha256=chunk.storage_sha256))
    service.commit_upload(commit)

    node_id = allocation.chunks[0].targets[0].node_id
    registry = service._registry
    registry._nodes[node_id] = replace(registry._nodes[node_id], state=pb.NODE_DEAD)

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.replication_state == pb.REPLICATION_UNAVAILABLE
    assert len(result.chunks[0].replicas) == 0
    assert len(result.chunks[1].replicas) == 1

def test_changing_response_does_not_change_stored_metadata_or_manifest():
    service, request, _ = prepare_file()
    query = pb.ResolveDownloadRequest(path=request.path)
    first = service.resolve_download(query)
    expected = pb.ResolveDownloadResponse.FromString(first.SerializeToString())
    first.file.path = "/alterado.txt"
    first.chunks[0].manifest.storage_sha256 = b"x" * 32
    first.chunks[0].replicas[0].node.grpc_target = "alterado:50052"

    assert service.resolve_download(query) == expected

def test_download_preserves_encrypted_manifest():
    service, request, _ = prepare_file(encrypted=True)

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.encryption == pb.ENCRYPTION_AES_256_GCM
    assert result.chunks[0].manifest.stored_size_bytes == 116
    assert result.chunks[0].manifest.nonce == b"\x01" * 12
    assert result.chunks[1].manifest.nonce == b"\x02" * 12

def test_download_supports_empty_file():
    service, request, _ = prepare_file(chunk_count=0)

    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

    assert result.file.plaintext_size_bytes == 0
    assert result.file.replication_state == pb.REPLICATION_HEALTHY
    assert len(result.chunks) == 0

def test_download_rejects_invalid_path():
    service, _, _ = prepare_file()

    with pytest.raises(AllocationError) as exc:
        service.resolve_download(pb.ResolveDownloadRequest(path="../teste.txt"))
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT

def test_grpc_download_adapter_returns_metadata_and_translates_errors():
    class Context:
        def abort(self, code, details):
            raise AllocationError(code, details)

    service, request, _ = prepare_file()

    result = service.ResolveDownload(pb.ResolveDownloadRequest(path=request.path), Context())
    assert result.file.state == pb.FILE_COMMITTED

    with pytest.raises(AllocationError) as exc:
        service.ResolveDownload(pb.ResolveDownloadRequest(path="/ausente.txt"), Context())
    assert exc.value.code == grpc.StatusCode.NOT_FOUND
