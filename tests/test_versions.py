import grpc
import pytest
from uuid import uuid4
import dcomp_drive_pb2 as pb
from metadata.service import AllocationError
from test_commit import prepare_upload, commit_request

def test_overwrite_publishes_new_version_and_preserves_old_retry():
    service, request, allocation, now = prepare_upload()
    old_commit = commit_request(request, allocation)
    first = service.commit_upload(old_commit)
    request.operation_id = str(uuid4())
    request.chunks[0].block_id = str(uuid4())
    request.overwrite = True
    request.expected_version = 1
    now[0] += 1
    second_allocation = service.allocate_upload(request)
    assert service.resolve_download(pb.ResolveDownloadRequest(path=request.path)).file.version == 1
    second = service.commit_upload(commit_request(request, second_allocation))
    assert second.version == 2
    assert second.file_id == first.file_id
    assert service.commit_upload(old_commit) == first
    assert service.resolve_download(pb.ResolveDownloadRequest(path=request.path)).chunks[0].manifest.block_id == request.chunks[0].block_id
    request.operation_id = str(uuid4())
    with pytest.raises(AllocationError) as exc:
        service.allocate_upload(request)
    assert exc.value.code == grpc.StatusCode.ABORTED
