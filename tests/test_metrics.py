import json
import urllib.request
from concurrent import futures

import grpc
import pytest
from google.protobuf.empty_pb2 import Empty
from prometheus_client import generate_latest

import dcomp_drive_pb2 as pb
import dcomp_drive_pb2_grpc as rpc
from common.metrics import Metrics, MetricsInterceptor, StatusCollector, start_metrics_server
from observability.fake_control import build_response
from observability.status_cli import status_to_dict


class _Control(rpc.ControlServiceServicer):
    def GetStatus(self, request, context):
        return build_response(0)

    def DrainNode(self, request, context):
        context.abort(grpc.StatusCode.NOT_FOUND, "no desconhecido")

    def Reconcile(self, request_iterator, context):
        total = sum(len(batch.blocks) for batch in request_iterator)
        return pb.ReconcileResponse(obsolete_block_ids=["x"] * total)


class _Storage(rpc.StorageServiceServicer):
    def GetBlock(self, request, context):
        yield pb.GetBlockFrame(header=pb.GetBlockHeader(block_id=request.block_id, stored_size_bytes=6))
        yield pb.GetBlockFrame(data=b"abc")
        yield pb.GetBlockFrame(data=b"def")


@pytest.fixture
def served():
    metrics = Metrics()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2), interceptors=[MetricsInterceptor(metrics, "teste")])
    rpc.add_ControlServiceServicer_to_server(_Control(), server)
    rpc.add_StorageServiceServicer_to_server(_Storage(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    yield metrics, channel
    channel.close()
    server.stop(0).wait()


def _count(metrics, method, code):
    return metrics.registry.get_sample_value(
        "dcomp_rpc_duration_seconds_count", {"service": _service(method), "method": method, "code": code}
    )


def _service(method):
    return "dcompdrive.v1.StorageService" if method == "GetBlock" else "dcompdrive.v1.ControlService"


def test_unario_ok_conta_duracao_e_bytes(served):
    metrics, channel = served
    rpc.ControlServiceStub(channel).GetStatus(Empty(), timeout=5)
    assert _count(metrics, "GetStatus", "OK") == 1
    assert metrics.registry.get_sample_value("dcomp_rpc_bytes_total", {"method": "GetStatus", "direction": "out"}) > 0


def test_abort_vira_codigo_do_grpc(served):
    metrics, channel = served
    with pytest.raises(grpc.RpcError):
        rpc.ControlServiceStub(channel).DrainNode(pb.DrainNodeRequest(node_id="x"), timeout=5)
    assert _count(metrics, "DrainNode", "NOT_FOUND") == 1


def test_stream_de_resposta_conta_bytes_so_no_fim(served):
    metrics, channel = served
    frames = list(rpc.StorageServiceStub(channel).GetBlock(pb.GetBlockRequest(block_id="b"), timeout=5))
    assert len(frames) == 3
    assert _count(metrics, "GetBlock", "OK") == 1
    assert metrics.registry.get_sample_value("dcomp_rpc_bytes_total", {"method": "GetBlock", "direction": "out"}) > 6


def test_stream_de_requisicao(served):
    metrics, channel = served
    batches = [pb.InventoryBatch(node_id="n", blocks=[pb.StoredBlockInfo(block_id="a")] * 2) for _ in range(3)]
    reply = rpc.ControlServiceStub(channel).Reconcile(iter(batches), timeout=5)
    assert len(reply.obsolete_block_ids) == 6
    assert _count(metrics, "Reconcile", "OK") == 1
    assert metrics.registry.get_sample_value("dcomp_rpc_bytes_total", {"method": "Reconcile", "direction": "in"}) > 0


def test_metricas_de_dominio_aparecem_no_texto():
    metrics = Metrics()
    metrics.corruptions_detected.labels(source="scrub").inc()
    metrics.recovery_bytes_copied.inc(4096)
    text = generate_latest(metrics.registry).decode()
    assert 'dcomp_corruptions_detected_total{source="scrub"} 1.0' in text
    assert "dcomp_recovery_bytes_copied_total 4096.0" in text


def test_status_collector_gera_gauges_do_cluster():
    metrics = Metrics()
    metrics.registry.register(StatusCollector(lambda: status_to_dict(build_response(2))))
    get = metrics.registry.get_sample_value
    assert get("dcomp_nodes", {"state": "ALIVE"}) == 4
    assert get("dcomp_nodes", {"state": "DEAD"}) == 1
    assert get("dcomp_files", {"replication_state": "UNDER_REPLICATED"}) == 6
    assert get("dcomp_cluster_recovery_jobs") == 2
    assert get("dcomp_status_scrape_ok") == 1


def test_status_collector_nao_derruba_o_scrape_quando_o_central_cai():
    metrics = Metrics()

    def broken():
        raise ConnectionError("central fora do ar")

    metrics.registry.register(StatusCollector(broken))
    assert metrics.registry.get_sample_value("dcomp_status_scrape_ok") == 0
    generate_latest(metrics.registry)


def test_endpoint_http_serve_texto_json_e_healthz():
    metrics = Metrics()
    metrics.uploads.labels(result="ok").inc(3)
    server = start_metrics_server(-1, metrics, host="127.0.0.1")
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        text = urllib.request.urlopen(f"{base}/metrics", timeout=5).read().decode()
        assert 'dcomp_uploads_total{result="ok"} 3.0' in text
        doc = json.loads(urllib.request.urlopen(f"{base}/metrics.json", timeout=5).read())
        names = {m["name"] for m in doc["metrics"]}
        assert "dcomp_uploads_total" in names and not any(n.endswith("_bucket") for n in names)
        assert urllib.request.urlopen(f"{base}/healthz", timeout=5).status == 200
        with pytest.raises(urllib.error.HTTPError):
            urllib.request.urlopen(f"{base}/nada", timeout=5)
    finally:
        server.shutdown()
        server.server_close()


def test_porta_zero_desliga_o_endpoint():
    assert start_metrics_server(0, Metrics()) is None

