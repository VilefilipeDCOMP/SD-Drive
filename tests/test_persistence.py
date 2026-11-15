import grpc
import pytest
import dcomp_drive_pb2 as pb
from metadata.service import MetadataService, AllocationError
from test_commit import prepare_upload, commit_request

def test_restart_preserves_pending_and_committed_state(tmp_path):
    original, request, _, now = prepare_upload()
    path = str(tmp_path / "metadata.db")
    service = MetadataService(original._registry, db_path=path, clock=lambda: now[0])
    allocation = service.allocate_upload(request)
    service.close()
    service = MetadataService(original._registry, db_path=path, clock=lambda: now[0])
    assert service.allocate_upload(request) == allocation
    result = service.commit_upload(commit_request(request, allocation))
    assert service._store.connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    service.close()
    service = MetadataService(original._registry, db_path=path, clock=lambda: now[0])
    assert service.resolve_download(pb.ResolveDownloadRequest(path=request.path)).file == result
    service.close()

def test_two_instances_observe_same_path_lease(tmp_path):
    original, request, _, _ = prepare_upload()
    path = str(tmp_path / "metadata.db")
    first = MetadataService(original._registry, db_path=path)
    second = MetadataService(original._registry, db_path=path)
    first.allocate_upload(request)
    from uuid import uuid4
    request.operation_id = str(uuid4())
    with pytest.raises(AllocationError) as exc:
        second.allocate_upload(request)
    assert exc.value.code == grpc.StatusCode.ABORTED
    first.close()
    second.close()

def test_failed_database_write_rolls_back_memory_and_disk(tmp_path, monkeypatch):
    original, request, _, _ = prepare_upload()
    path = str(tmp_path / "metadata.db")
    service = MetadataService(original._registry, db_path=path)
    save = service._store.save
    def fail(state):
        raise OSError("Falha simulada de disco")
    monkeypatch.setattr(service._store, "save", fail)
    with pytest.raises(OSError):
        service.allocate_upload(request)
    assert not service._operations
    assert not service._reserved
    monkeypatch.setattr(service._store, "save", save)
    assert service.allocate_upload(request).upload_id
    service.close()
