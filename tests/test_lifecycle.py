import grpc
import pytest
import dcomp_drive_pb2 as pb
from metadata.service import AllocationError
from test_commit import prepare_upload, commit_request

class Context:
    def abort(self, code, details):
        raise AllocationError(code, details)

def test_abort_is_idempotent_and_invalidates_attempt():
    service, request, allocation, _ = prepare_upload()
    query = pb.AbortUploadRequest(upload_id=allocation.upload_id)
    service.AbortUpload(query, Context())
    service.AbortUpload(query, Context())
    assert not service._reserved
    with pytest.raises(AllocationError):
        service.commit_upload(commit_request(request, allocation))
    with pytest.raises(AllocationError):
        service.allocate_upload(request)

def test_abort_rejects_committed_upload():
    service, request, allocation, _ = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    with pytest.raises(AllocationError) as exc:
        service.AbortUpload(pb.AbortUploadRequest(upload_id=allocation.upload_id), Context())
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
