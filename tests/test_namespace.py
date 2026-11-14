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

def test_stat_reads_file_and_root_without_exposing_pending():
    service, request, allocation, _ = prepare_upload()
    assert service.Stat(pb.StatRequest(path="/"), Context()).is_directory
    with pytest.raises(AllocationError):
        service.Stat(pb.StatRequest(path=request.path), Context())
    service.commit_upload(commit_request(request, allocation))
    result = service.Stat(pb.StatRequest(path=request.path), Context())
    assert result.file.state == pb.FILE_COMMITTED
    assert len(result.chunks) == 1

def test_listdir_paginates_visible_children():
    service, request, allocation, _ = prepare_upload()
    service.Mkdir(pb.MkdirRequest(path="/pasta"), Context())
    assert [e.path for e in service.ListDir(pb.ListDirRequest(path="/"), Context()).entries] == ["/pasta"]
    service.commit_upload(commit_request(request, allocation))
    first = service.ListDir(pb.ListDirRequest(path="/", page_size=1), Context())
    second = service.ListDir(pb.ListDirRequest(path="/", page_size=1, page_token=first.next_page_token), Context())
    assert [e.path for e in first.entries] == ["/pasta"]
    assert [e.path for e in second.entries] == ["/teste.txt"]
    assert not second.next_page_token
