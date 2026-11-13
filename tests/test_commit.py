from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import grpc
import pytest

import dcomp_drive_pb2 as pb
from common.ports import NodeSnapshot
from common.simulated_registry import SimulatedNodeRegistry
from metadata.service import AllocationError, MetadataService

def prepare_upload(*, chunk_count=1, write_quorum="all"):
    nodes = []
    for index in range(3):
        nodes.append(NodeSnapshot(str(uuid4()), f"machine-{index}", f"disk-{index}", f"storage-{index}:50052", pb.NODE_ALIVE, 10000, 0, 10000))

    now = [1000.0]
    service = MetadataService(SimulatedNodeRegistry(nodes), write_quorum=write_quorum, clock=lambda: now[0], lease_seconds=10)
    request = pb.AllocateUploadRequest(path="/teste.txt", operation_id=str(uuid4()), plaintext_size_bytes=100 * chunk_count, plaintext_sha256=b"a" * 32, encryption=pb.ENCRYPTION_NONE)
    for index in range(chunk_count):
        request.chunks.append(pb.ChunkManifest(block_id=str(uuid4()), index=index, stored_size_bytes=100, storage_sha256=b"b" * 32))

    allocation = service.allocate_upload(request)
    return service, request, allocation, now

def commit_request(request, allocation, copies=3):
    commit = pb.CommitUploadRequest(upload_id=allocation.upload_id)
    for chunk, placement in zip(request.chunks, allocation.chunks):
        for node in placement.targets[:copies]:
            commit.receipts.append(pb.BlockReceipt(block_id=chunk.block_id, node_id=node.node_id, stored_size_bytes=chunk.stored_size_bytes, verified_storage_sha256=chunk.storage_sha256))
    return commit

def test_commit_returns_metadata_matching_allocation_and_manifest():
    service, request, allocation, now = prepare_upload(chunk_count=2)
    now[0] += 1

    result = service.commit_upload(commit_request(request, allocation))

    assert result.file_id == allocation.file_id
    assert result.version == allocation.version
    assert result.path == "/teste.txt"
    assert result.plaintext_size_bytes == 200
    assert result.plaintext_sha256 == b"a" * 32
    assert result.encryption == pb.ENCRYPTION_NONE
    assert result.state == pb.FILE_COMMITTED
    assert result.replication_state == pb.REPLICATION_HEALTHY
    assert result.desired_replica_count == 3
    assert result.created_at_unix_ms == result.updated_at_unix_ms == 1001000

def test_quorum_must_be_satisfied_for_each_chunk_without_partial_publication():
    service, request, allocation, _ = prepare_upload(chunk_count=2)
    commit = commit_request(request, allocation)
    del commit.receipts[-1]

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit)
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert request.path not in service._files

    assert service.commit_upload(commit_request(request, allocation)).state == pb.FILE_COMMITTED

def test_duplicate_receipts_do_not_count_as_distinct_replicas():
    service, request, allocation, _ = prepare_upload()
    commit = commit_request(request, allocation, copies=1)
    commit.receipts.append(commit.receipts[0])
    commit.receipts.append(commit.receipts[0])

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit)
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert request.path not in service._files

@pytest.mark.parametrize("field,value", [("block_id", "unknown-block"), ("node_id", "unknown-node"), ("stored_size_bytes", 99), ("verified_storage_sha256", b"c" * 32)])
def test_commit_rejects_receipt_that_does_not_match_plan(field, value):
    service, request, allocation, _ = prepare_upload()
    commit = commit_request(request, allocation)
    setattr(commit.receipts[-1], field, value)

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit)
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert request.path not in service._files

def test_commit_rejects_expired_lease():
    service, request, allocation, now = prepare_upload()
    now[0] += 10

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit_request(request, allocation))
    assert exc.value.code == grpc.StatusCode.ABORTED
    assert request.path not in service._files

def test_commit_rejects_unknown_upload():
    service, _, _, _ = prepare_upload()

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(pb.CommitUploadRequest(upload_id=str(uuid4())))
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION

def test_smaller_write_quorum_publishes_under_replicated_file():
    service, request, allocation, _ = prepare_upload(write_quorum=2)

    result = service.commit_upload(commit_request(request, allocation, copies=2))

    assert result.state == pb.FILE_COMMITTED
    assert result.replication_state == pb.REPLICATION_UNDER_REPLICATED

def test_commit_retry_is_idempotent_even_after_original_lease_deadline():
    service, request, allocation, now = prepare_upload()
    commit = commit_request(request, allocation)
    first = service.commit_upload(commit)
    expected = pb.FileMetadata.FromString(first.SerializeToString())
    first.path = "/alterado.txt"
    commit.receipts.reverse()
    commit.receipts.append(commit.receipts[0])
    now[0] += 20

    assert service.commit_upload(commit) == expected

def test_confirmed_file_cannot_be_allocated_again_before_overwrite_is_implemented():
    service, request, allocation, _ = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    request.operation_id = str(uuid4())
    request.chunks[0].block_id = str(uuid4())

    with pytest.raises(AllocationError) as exc:
        service.allocate_upload(request)
    assert exc.value.code == grpc.StatusCode.ALREADY_EXISTS

def test_committed_capacity_is_not_released_when_lease_deadline_passes():
    service, request, allocation, now = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    now[0] += 20
    other = pb.AllocateUploadRequest(path="/outro.txt", operation_id=str(uuid4()), plaintext_size_bytes=9000, plaintext_sha256=b"a" * 32, encryption=pb.ENCRYPTION_NONE, chunks=[pb.ChunkManifest(block_id=str(uuid4()), index=0, stored_size_bytes=9000, storage_sha256=b"b" * 32)])

    with pytest.raises(AllocationError) as exc:
        service.allocate_upload(other)
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION

def test_concurrent_commits_return_the_same_published_metadata():
    service, request, allocation, _ = prepare_upload()
    commit = commit_request(request, allocation)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(service.commit_upload, [commit] * 4))

    for result in results:
        assert result == results[0]
    assert len(service._files) == 1

def test_grpc_commit_adapter_returns_metadata_and_translates_errors():
    class Context:
        def abort(self, code, details):
            raise AllocationError(code, details)

    service, request, allocation, _ = prepare_upload()
    result = service.CommitUpload(commit_request(request, allocation), Context())
    assert result.state == pb.FILE_COMMITTED

    with pytest.raises(AllocationError) as exc:
        service.CommitUpload(pb.CommitUploadRequest(upload_id=str(uuid4())), Context())
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION

def test_commit_rejects_invalid_extra_receipt_even_with_enough_valid_receipts():
    service, request, allocation, _ = prepare_upload()
    commit = commit_request(request, allocation)
    commit.receipts.append(pb.BlockReceipt(block_id=request.chunks[0].block_id, node_id=str(uuid4()), stored_size_bytes=100, verified_storage_sha256=b"b" * 32))

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit)
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert request.path not in service._files

def test_commit_retry_cannot_change_the_confirmed_replicas():
    service, request, allocation, _ = prepare_upload(write_quorum=2)
    original = commit_request(request, allocation, copies=2)
    first = service.commit_upload(original)

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit_request(request, allocation, copies=3))
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert service.commit_upload(original) == first

@pytest.mark.parametrize("dead_count,expected", [(1, pb.REPLICATION_UNDER_REPLICATED), (3, pb.REPLICATION_UNAVAILABLE)])
def test_commit_reports_replication_using_current_node_states(dead_count, expected):
    service, request, allocation, _ = prepare_upload()
    registry = service._registry
    for node in allocation.chunks[0].targets[:dead_count]:
        registry._nodes[node.node_id] = replace(registry._nodes[node.node_id], state=pb.NODE_DEAD)

    result = service.commit_upload(commit_request(request, allocation))

    assert result.state == pb.FILE_COMMITTED
    assert result.replication_state == expected

def test_lease_must_still_be_valid_at_publication(monkeypatch):
    service, request, allocation, now = prepare_upload()
    get_node_state = service._registry.get_node_state

    def expire_lease(node_id):
        now[0] = 1010.0
        return get_node_state(node_id)

    monkeypatch.setattr(service._registry, "get_node_state", expire_lease)

    with pytest.raises(AllocationError) as exc:
        service.commit_upload(commit_request(request, allocation))
    assert exc.value.code == grpc.StatusCode.ABORTED
    assert request.path not in service._files

def test_overwrite_keeps_previous_file_visible_until_commit():
    service, request, allocation, _ = prepare_upload()
    commit = commit_request(request, allocation)
    first = service.commit_upload(commit)
    request.operation_id = str(uuid4())
    request.chunks[0].block_id = str(uuid4())
    request.overwrite = True

    replacement = service.allocate_upload(request)
    visible = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))
    assert visible.file == first
    assert service.commit_upload(commit) == first
    assert replacement.version == 2
