from concurrent.futures import ThreadPoolExecutor
import grpc
import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc
from uuid import uuid4
import pytest
from metadata.bootstrap import register_metadata
from test_commit import prepare_upload, commit_request

def test_registered_service_uses_real_grpc_and_configured_database(tmp_path):
    original, request, _, _ = prepare_upload()
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    service = register_metadata(server, original._registry, {"cluster": {"replication_factor": 2}, "controller": {"metadata_db_path": str(tmp_path / "metadata.db")}})
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = rpc.MetadataServiceStub(channel)
            allocation = stub.AllocateUpload(request, timeout=5)
            assert allocation.write_quorum == 2
            metadata = stub.CommitUpload(commit_request(request, allocation, copies=2), timeout=5)
            assert stub.ResolveDownload(pb.ResolveDownloadRequest(path=request.path), timeout=5).file == metadata
            stub.Mkdir(pb.MkdirRequest(path="/docs"), timeout=5)
            stub.Rename(pb.RenameRequest(source_path=request.path, target_path="/docs/a.txt"), timeout=5)
            assert stub.Stat(pb.StatRequest(path="/docs/a.txt"), timeout=5).file.file_id == metadata.file_id
            assert stub.ListDir(pb.ListDirRequest(path="/docs"), timeout=5).entries[0].path == "/docs/a.txt"
            stub.Delete(pb.DeleteRequest(path="/docs/a.txt", expected_version=1), timeout=5)
            stub.Rmdir(pb.RmdirRequest(path="/docs"), timeout=5)
            request.path = "/cancelado.txt"
            request.operation_id = str(uuid4())
            request.chunks[0].block_id = str(uuid4())
            pending = stub.AllocateUpload(request, timeout=5)
            stub.RenewLease(pb.RenewLeaseRequest(upload_id=pending.upload_id), timeout=5)
            stub.AbortUpload(pb.AbortUploadRequest(upload_id=pending.upload_id), timeout=5)
            with pytest.raises(grpc.RpcError) as exc:
                stub.ResolveDownload(pb.ResolveDownloadRequest(path=request.path), timeout=5)
            assert exc.value.code() == grpc.StatusCode.NOT_FOUND
    finally:
        server.stop(0).wait()
        service.close()
