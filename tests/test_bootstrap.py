from concurrent.futures import ThreadPoolExecutor
import grpc
import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc
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
    finally:
        server.stop(0).wait()
        service.close()
