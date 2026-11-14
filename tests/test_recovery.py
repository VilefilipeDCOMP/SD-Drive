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
