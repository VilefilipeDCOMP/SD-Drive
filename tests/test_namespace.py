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

def test_rename_moves_directory_tree_without_changing_blocks():
    service, request, allocation, _ = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    service.Mkdir(pb.MkdirRequest(path="/pasta"), Context())
    service.Rename(pb.RenameRequest(source_path=request.path, target_path="/pasta/a.txt"), Context())
    service.Rename(pb.RenameRequest(source_path="/pasta", target_path="/nova"), Context())
    result = service.resolve_download(pb.ResolveDownloadRequest(path="/nova/a.txt"))
    assert result.chunks[0].manifest.block_id == request.chunks[0].block_id
    with pytest.raises(AllocationError):
        service.resolve_download(pb.ResolveDownloadRequest(path=request.path))

def test_delete_checks_version_and_hides_file():
    service, request, allocation, _ = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    with pytest.raises(AllocationError) as exc:
        service.Delete(pb.DeleteRequest(path=request.path, expected_version=2), Context())
    assert exc.value.code == grpc.StatusCode.ABORTED
    service.Delete(pb.DeleteRequest(path=request.path, expected_version=1), Context())
    with pytest.raises(AllocationError):
        service.resolve_download(pb.ResolveDownloadRequest(path=request.path))
    assert not service.ListDir(pb.ListDirRequest(path="/"), Context()).entries

def test_rmdir_rejects_nonempty_then_removes_tree_recursively():
    service, request, allocation, _ = prepare_upload()
    service.commit_upload(commit_request(request, allocation))
    service.Mkdir(pb.MkdirRequest(path="/pasta"), Context())
    service.Rename(pb.RenameRequest(source_path=request.path, target_path="/pasta/a.txt"), Context())
    with pytest.raises(AllocationError) as exc:
        service.Rmdir(pb.RmdirRequest(path="/pasta"), Context())
    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    service.Rmdir(pb.RmdirRequest(path="/pasta", recursive=True), Context())
    assert not service.ListDir(pb.ListDirRequest(path="/"), Context()).entries
