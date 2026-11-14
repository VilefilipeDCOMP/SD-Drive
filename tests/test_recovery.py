import dcomp_drive_pb2 as pb
from test_commit import prepare_upload, commit_request
from test_lifecycle import Context

def test_recovery_queries_only_reference_active_committed_blocks():
    service, request, allocation, _ = prepare_upload(write_quorum=2)
    assert service.list_under_replicated_blocks() == []
    service.commit_upload(commit_request(request, allocation, copies=2))
    block_id = request.chunks[0].block_id
    assert len(service.list_healthy_sources(block_id)) == 2
    assert service.list_under_replicated_blocks()[0]["live_replica_count"] == 2
    node_id = allocation.chunks[0].targets[0].node_id
    assert service.list_blocks_on_node(node_id)[0]["block_id"] == block_id
    service.Delete(pb.DeleteRequest(path=request.path), Context())
    assert service.list_blocks_on_node(node_id) == []

def test_recovery_destination_respects_existing_domains_and_exclusions():
    service, request, allocation, _ = prepare_upload(write_quorum=2)
    service.commit_upload(commit_request(request, allocation, copies=2))
    destination = service.select_destination(request.chunks[0].block_id, set())
    assert destination.node_id == allocation.chunks[0].targets[2].node_id
    import pytest
    from metadata.service import AllocationError
    with pytest.raises(AllocationError):
        service.select_destination(request.chunks[0].block_id, {destination.node_id})

def test_invalid_replica_is_excluded_from_download_and_sources():
    service, request, allocation, _ = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    block_id = request.chunks[0].block_id
    node_id = allocation.chunks[0].targets[0].node_id
    service.mark_replica_invalid(block_id, node_id)
    service.mark_replica_invalid(block_id, node_id)
    assert len(service.list_healthy_sources(block_id)) == 2
    result = service.resolve_download(pb.ResolveDownloadRequest(path=request.path))
    assert len(result.chunks[0].replicas) == 2
    assert result.file.replication_state == pb.REPLICATION_UNDER_REPLICATED

def test_recovered_receipt_restores_replication_without_breaking_commit_retry():
    service, request, allocation, _ = prepare_upload(write_quorum=2)
    original = commit_request(request, allocation, copies=2)
    result = service.commit_upload(original)
    target = service.select_destination(request.chunks[0].block_id, set())
    receipt = pb.BlockReceipt(block_id=request.chunks[0].block_id, node_id=target.node_id, stored_size_bytes=100, verified_storage_sha256=b"b" * 32)
    service.register_recovered_replica(receipt.block_id, receipt)
    service.register_recovered_replica(receipt.block_id, receipt)
    assert service.list_under_replicated_blocks() == []
    assert service.commit_upload(original) == result
    assert len(service.resolve_download(pb.ResolveDownloadRequest(path=request.path)).chunks[0].replicas) == 3

def test_gc_protects_active_versions_and_pending_but_exposes_deleted_blocks():
    service, request, allocation, _ = prepare_upload()
    assert service.gc_candidates() == []
    service.commit_upload(commit_request(request, allocation))
    assert service.gc_candidates() == []
    service.Delete(pb.DeleteRequest(path=request.path), Context())
    candidates = service.gc_candidates()
    assert len(candidates) == 3
    assert {item["block_id"] for item in candidates} == {request.chunks[0].block_id}

def test_gc_includes_expired_and_cancelled_partial_uploads():
    service, request, allocation, now = prepare_upload()
    now[0] += 10
    assert len(service.gc_candidates()) == 3
    assert not service._reserved
