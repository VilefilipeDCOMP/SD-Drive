import grpc
import pytest
import dcomp_drive_pb2 as pb
from metadata.service import AllocationError
from test_commit import prepare_upload, commit_request
from test_lifecycle import Context

def test_mkdir_enables_nested_upload_and_rejects_missing_parent():
    service, request, _, _ = prepare_upload()
    service.Mkdir(pb.MkdirRequest(path="/pasta"), Context())
    from uuid import uuid4
    request.path = "/pasta/arquivo.txt"
    request.operation_id = str(uuid4())
    request.chunks[0].block_id = str(uuid4())
    assert service.allocate_upload(request).upload_id
    with pytest.raises(AllocationError) as exc:
        service.Mkdir(pb.MkdirRequest(path="/ausente/filha"), Context())
    assert exc.value.code == grpc.StatusCode.NOT_FOUND
