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

def test_namespace_versions_tombstones_and_gc_survive_restart(tmp_path):
    from test_lifecycle import Context
    from uuid import uuid4
    original, request, _, now = prepare_upload()
    path = str(tmp_path / "metadata.db")
    service = MetadataService(original._registry, db_path=path, clock=lambda: now[0])
    first = service.allocate_upload(request)
    service.commit_upload(commit_request(request, first))
    request.operation_id = str(uuid4())
    request.chunks[0].block_id = str(uuid4())
    request.overwrite = True
    request.expected_version = 1
    second = service.allocate_upload(request)
    service.commit_upload(commit_request(request, second))
    service.Mkdir(pb.MkdirRequest(path="/docs"), Context())
    service.Rename(pb.RenameRequest(source_path=request.path, target_path="/docs/novo.txt"), Context())
    service.close()
    service = MetadataService(original._registry, db_path=path, clock=lambda: now[0])
    assert service.Stat(pb.StatRequest(path="/docs/novo.txt"), Context()).file.version == 2
    assert len(service.gc_candidates()) == 3
    service.Delete(pb.DeleteRequest(path="/docs/novo.txt", expected_version=2), Context())
    service.close()
    service = MetadataService(original._registry, db_path=path, clock=lambda: now[0])
    assert len(service.gc_candidates()) == 6
    for item in service.gc_candidates():
        service.acknowledge_gc(item["block_id"], item["node"].node_id)
    assert not service._reserved
    service.close()
