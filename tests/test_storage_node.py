from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid
from concurrent import futures
from pathlib import Path

import grpc
import pytest

pb = pytest.importorskip("dcomp_drive_pb2")
pb_grpc = pytest.importorskip("dcomp_drive_pb2_grpc")

from google.protobuf import empty_pb2

from storage import corrupt_tool
from storage.config import StorageConfig
from storage.node import StorageNode

FRAME = 64 * 1024


class FakeController(pb_grpc.ControlServiceServicer):
    def __init__(self):
        self.registers = []
        self.heartbeats = []
        self.reports = []
        self.inventories = []
        self.obsolete = []
        self.fail_heartbeat_not_found = False
        self.reject_reports = False
        self.lock = threading.Lock()

    def RegisterNode(self, request, context):
        with self.lock:
            self.registers.append(request)
        return pb.RegisterNodeResponse(heartbeat_interval_seconds=1, suspect_timeout_seconds=10,
                                       dead_timeout_seconds=30)

    def Heartbeat(self, request, context):
        if self.fail_heartbeat_not_found:
            context.abort(grpc.StatusCode.NOT_FOUND, "nó desconhecido")
        with self.lock:
            self.heartbeats.append(request)
        return empty_pb2.Empty()

    def ReportCorruption(self, request, context):
        if self.reject_reports:
            context.abort(grpc.StatusCode.UNAVAILABLE, "indisponível")
        with self.lock:
            self.reports.append(request)
        return empty_pb2.Empty()

    def Reconcile(self, request_iterator, context):
        batches = list(request_iterator)
        with self.lock:
            self.inventories.append(batches)
        return pb.ReconcileResponse(obsolete_block_ids=list(self.obsolete))


@pytest.fixture
def controller():
    fake = FakeController()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_ControlServiceServicer_to_server(fake, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    fake.target = f"127.0.0.1:{port}"
    yield fake
    server.stop(0)


def make_cfg(tmp: Path, controller_target: str, **kw) -> StorageConfig:
    base = dict(data_dir=tmp, bind="127.0.0.1:0", controller_target=controller_target, capacity_bytes=64 * 1024 ** 2,
                frame_size=FRAME, scrub_interval=0, metrics_port=-1, machine_id="m1", disk_id="d1",
                fsync=False, controller_deadline=3.0, copy_deadline=20.0)
    base.update(kw)
    return StorageConfig(**base)


@pytest.fixture
def node(tmp_path, controller):
    n = StorageNode(make_cfg(tmp_path / "n1", controller.target)).start()
    yield n
    n.stop(0.5)


@pytest.fixture
def stub(node):
    ch = grpc.insecure_channel(node.advertise)
    yield pb_grpc.StorageServiceStub(ch)
    ch.close()


def sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def new_id() -> str:
    return str(uuid.uuid4())


def store_frames(block_id, data, *, size=None, digest=None, upload_id="up-1", version=1, frame=FRAME):
    yield pb.StoreBlockFrame(header=pb.StoreBlockHeader(
        block_id=block_id, upload_id=upload_id, version=version,
        expected_size_bytes=len(data) if size is None else size,
        expected_storage_sha256=sha(data) if digest is None else digest))
    for i in range(0, len(data), frame):
        yield pb.StoreBlockFrame(data=data[i:i + frame])


def put(stub, block_id, data, **kw):
    return stub.StoreBlock(store_frames(block_id, data, **kw), timeout=15)


def get(stub, block_id) -> bytes:
    frames = list(stub.GetBlock(pb.GetBlockRequest(block_id=block_id), timeout=15))
    assert frames[0].WhichOneof("payload") == "header"
    body = b"".join(f.data for f in frames[1:])
    assert all(f.WhichOneof("payload") == "data" for f in frames[1:])
    return body


def code_of(exc_info) -> grpc.StatusCode:
    return exc_info.value.code()


def wait_for(cond, timeout=8.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


def blk_path(node, block_id) -> Path:
    return node.store.path_for(block_id)


def flip_byte(path: Path, offset=10):
    with open(path, "r+b") as f:
        f.seek(offset)
        b = f.read(1)
        f.seek(offset)
        f.write(bytes([b[0] ^ 0xFF]))


def test_roundtrip_multiframe(stub, node):
    data = os.urandom(FRAME * 5 + 123)
    bid = new_id()
    receipt = put(stub, bid, data)
    assert receipt.block_id == bid
    assert receipt.node_id == node.identity.node_id
    assert receipt.stored_size_bytes == len(data)
    assert receipt.verified_storage_sha256 == sha(data)
    assert blk_path(node, bid).read_bytes() == data
    assert get(stub, bid) == data
    frames = list(stub.GetBlock(pb.GetBlockRequest(block_id=bid)))
    assert frames[0].header.stored_size_bytes == len(data)
    assert frames[0].header.storage_sha256 == sha(data)
    assert max(len(f.data) for f in frames[1:]) <= FRAME


def test_empty_block(stub):
    bid = new_id()
    assert put(stub, bid, b"").stored_size_bytes == 0
    assert get(stub, bid) == b""


def test_hash_mismatch_rejected_and_nothing_left(stub, node):
    data = os.urandom(200_000)
    bid = new_id()
    with pytest.raises(grpc.RpcError) as e:
        put(stub, bid, data, digest=sha(b"outro conteudo"))
    assert code_of(e) == grpc.StatusCode.DATA_LOSS
    assert node.store.get_record(bid) is None
    assert not blk_path(node, bid).exists()
    assert list(node.store.tmp_dir.iterdir()) == []
    assert node.store.usage().used_bytes == 0


def test_size_mismatch_rejected(stub, node):
    data = os.urandom(100_000)
    for declared in (len(data) - 1, len(data) + 1):
        bid = new_id()
        with pytest.raises(grpc.RpcError) as e:
            put(stub, bid, data, size=declared)
        assert code_of(e) == grpc.StatusCode.INVALID_ARGUMENT
        assert node.store.get_record(bid) is None
    assert list(node.store.tmp_dir.iterdir()) == []
    assert node.store.usage().used_bytes == 0


def test_protocol_violations(stub, node):
    bid = new_id()
    with pytest.raises(grpc.RpcError) as e:
        stub.StoreBlock(iter([pb.StoreBlockFrame(data=b"abc")]), timeout=5)
    assert code_of(e) == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as e:
        stub.StoreBlock(iter([]), timeout=5)
    assert code_of(e) == grpc.StatusCode.INVALID_ARGUMENT
    data = b"x" * 1000

    def two_headers():
        h = pb.StoreBlockHeader(block_id=bid, expected_size_bytes=len(data), expected_storage_sha256=sha(data))
        yield pb.StoreBlockFrame(header=h)
        yield pb.StoreBlockFrame(header=h)

    with pytest.raises(grpc.RpcError) as e:
        stub.StoreBlock(two_headers(), timeout=5)
    assert code_of(e) == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as e:
        put(stub, bid, data, digest=b"curto")
    assert code_of(e) == grpc.StatusCode.INVALID_ARGUMENT
    for evil in ("../escape", "a/b", "", "..", "x" * 200):
        with pytest.raises(grpc.RpcError) as e:
            put(stub, evil, data)
        assert code_of(e) == grpc.StatusCode.INVALID_ARGUMENT
    assert node.store.get_record(bid) is None
    assert list(node.store.tmp_dir.iterdir()) == []


def test_idempotent_store_and_conflict(stub, node):
    data = os.urandom(150_000)
    bid = new_id()
    r1 = put(stub, bid, data)
    r2 = put(stub, bid, data, upload_id="up-retry")
    assert r1.verified_storage_sha256 == r2.verified_storage_sha256 == sha(data)
    assert node.store.usage().used_bytes == len(data)
    assert node.store.usage().blocks == 1
    with pytest.raises(grpc.RpcError) as e:
        put(stub, bid, os.urandom(150_000))
    assert code_of(e) == grpc.StatusCode.ALREADY_EXISTS
    assert get(stub, bid) == data


def test_get_missing_and_delete_idempotent(stub, node):
    with pytest.raises(grpc.RpcError) as e:
        get(stub, new_id())
    assert code_of(e) == grpc.StatusCode.NOT_FOUND
    data = os.urandom(10_000)
    bid = new_id()
    put(stub, bid, data)
    assert node.store.usage().used_bytes == len(data)
    stub.DeleteBlock(pb.DeleteBlockRequest(block_id=bid))
    stub.DeleteBlock(pb.DeleteBlockRequest(block_id=bid))
    stub.DeleteBlock(pb.DeleteBlockRequest(block_id=new_id()))
    assert node.store.usage().used_bytes == 0
    assert not blk_path(node, bid).exists()
    assert not blk_path(node, bid).with_suffix(".meta").exists()
    with pytest.raises(grpc.RpcError) as e:
        get(stub, bid)
    assert code_of(e) == grpc.StatusCode.NOT_FOUND


def test_client_abort_mid_stream_leaves_nothing(stub, node):
    data = os.urandom(FRAME * 4)
    bid = new_id()

    def broken():
        yield pb.StoreBlockFrame(header=pb.StoreBlockHeader(
            block_id=bid, expected_size_bytes=len(data), expected_storage_sha256=sha(data)))
        yield pb.StoreBlockFrame(data=data[:FRAME])
        raise RuntimeError("cliente caiu")

    with pytest.raises(grpc.RpcError):
        stub.StoreBlock(broken(), timeout=5)
    assert wait_for(lambda: node.store.usage().used_bytes == 0)
    assert wait_for(lambda: list(node.store.tmp_dir.iterdir()) == [])
    assert node.store.get_record(bid) is None
    assert put(stub, bid, data).stored_size_bytes == len(data)

def test_corruption_on_read_detected_reported_and_discarded(stub, node, controller):
    data = os.urandom(300_000)
    bid = new_id()
    put(stub, bid, data)
    flip_byte(blk_path(node, bid))
    with pytest.raises(grpc.RpcError) as e:
        get(stub, bid)
    assert code_of(e) == grpc.StatusCode.DATA_LOSS
    assert wait_for(lambda: any(r.block_id == bid for r in controller.reports))
    rep = next(r for r in controller.reports if r.block_id == bid)
    assert rep.suspected_node_id == node.identity.node_id
    assert rep.observed_sha256 != sha(data) and len(rep.observed_sha256) == 32
    assert wait_for(lambda: node.store.get_record(bid) is None)
    assert node.store.usage().used_bytes == 0
    with pytest.raises(grpc.RpcError) as e:
        get(stub, bid)
    assert code_of(e) == grpc.StatusCode.NOT_FOUND
    put(stub, bid, data)
    assert get(stub, bid) == data


def test_corrupt_block_is_replaced_by_valid_store_even_if_report_pending(tmp_path, controller):
    controller.reject_reports = True
    node = StorageNode(make_cfg(tmp_path / "n", controller.target)).start()
    try:
        ch = grpc.insecure_channel(node.advertise)
        stub = pb_grpc.StorageServiceStub(ch)
        data = os.urandom(50_000)
        bid = new_id()
        put(stub, bid, data)
        flip_byte(blk_path(node, bid))
        with pytest.raises(grpc.RpcError):
            get(stub, bid)
        assert node.store.get_record(bid).corrupt
        assert node.link.pending_reports() == 1
        put(stub, bid, data)
        assert not node.store.get_record(bid).corrupt
        assert get(stub, bid) == data
        controller.reject_reports = False
        assert wait_for(lambda: any(r.block_id == bid for r in controller.reports), timeout=10)
        time.sleep(0.3)
        assert node.store.get_record(bid) is not None
        assert get(stub, bid) == data
        ch.close()
    finally:
        node.stop(0.5)


def test_scrubber_detects_corruption_and_missing_file(stub, node, controller):
    good, bad, gone = new_id(), new_id(), new_id()
    for b in (good, bad, gone):
        put(stub, b, os.urandom(120_000))
    flip_byte(blk_path(node, bad), 5)
    blk_path(node, gone).unlink()
    summary = node.scrubber.run_pass()
    assert summary["blocks"] == 3 and summary["corrupt"] == 1 and summary["missing"] == 1
    assert wait_for(lambda: {r.block_id for r in controller.reports} >= {bad, gone})
    missing_report = next(r for r in controller.reports if r.block_id == gone)
    assert missing_report.observed_sha256 == b""
    assert node.store.get_record(good) is not None
    assert get(stub, good) is not None


def test_scrubber_thread_runs_periodically(tmp_path, controller):
    node = StorageNode(make_cfg(tmp_path / "s", controller.target, scrub_interval=0.3)).start()
    try:
        ch = grpc.insecure_channel(node.advertise)
        stub = pb_grpc.StorageServiceStub(ch)
        bid = new_id()
        put(stub, bid, os.urandom(40_000))
        flip_byte(blk_path(node, bid), 3)
        assert wait_for(lambda: node.store.get_record(bid) is None or node.store.get_record(bid).corrupt, 5)
        assert wait_for(lambda: any(r.block_id == bid for r in controller.reports), 5)
        ch.close()
    finally:
        node.stop(0.5)


def test_truncated_block_detected_on_startup(tmp_path, controller):
    cfg = make_cfg(tmp_path / "t", controller.target)
    n = StorageNode(cfg).start()
    ch = grpc.insecure_channel(n.advertise)
    stub = pb_grpc.StorageServiceStub(ch)
    bid = new_id()
    put(stub, bid, os.urandom(100_000))
    ch.close()
    n.stop(0.5)
    with open(blk_path(n, bid), "r+b") as f:
        f.truncate(1000)
    n2 = StorageNode(cfg).start()
    try:
        assert n2.store.get_record(bid).corrupt
        assert wait_for(lambda: any(r.block_id == bid for r in controller.reports))
    finally:
        n2.stop(0.5)


def test_copy_between_nodes(tmp_path, controller, node, stub):
    dst = StorageNode(make_cfg(tmp_path / "n2", controller.target, machine_id="m2", disk_id="d2")).start()
    try:
        data = os.urandom(FRAME * 3 + 7)
        bid = new_id()
        put(stub, bid, data, version=7)
        req = pb.CopyBlockRequest(
            block_id=bid, destination=pb.NodeAddress(node_id=dst.identity.node_id, grpc_target=dst.advertise),
            expected_storage_sha256=sha(data), stored_size_bytes=len(data), version=7)
        receipt = stub.CopyBlock(req, timeout=15)
        assert receipt.node_id == dst.identity.node_id
        assert receipt.verified_storage_sha256 == sha(data)
        assert receipt.stored_size_bytes == len(data)
        assert dst.store.path_for(bid).read_bytes() == data
        assert dst.store.get_record(bid).version == 7
        assert stub.CopyBlock(req, timeout=15).verified_storage_sha256 == sha(data)
        assert dst.store.usage().blocks == 1
    finally:
        dst.stop(0.5)


def test_copy_error_paths(tmp_path, controller, node, stub):
    dst = StorageNode(make_cfg(tmp_path / "n2", controller.target)).start()
    try:
        data = os.urandom(80_000)
        bid = new_id()
        put(stub, bid, data)
        dest = pb.NodeAddress(node_id=dst.identity.node_id, grpc_target=dst.advertise)

        def copy(**over):
            kw = dict(block_id=bid, destination=dest, expected_storage_sha256=sha(data),
                      stored_size_bytes=len(data), version=1)
            kw.update(over)
            return stub.CopyBlock(pb.CopyBlockRequest(**kw), timeout=15)

        with pytest.raises(grpc.RpcError) as e:
            copy(block_id=new_id())
        assert code_of(e) == grpc.StatusCode.NOT_FOUND
        with pytest.raises(grpc.RpcError) as e:
            copy(expected_storage_sha256=sha(b"x"))
        assert code_of(e) == grpc.StatusCode.FAILED_PRECONDITION
        with pytest.raises(grpc.RpcError) as e:
            copy(destination=pb.NodeAddress(node_id=node.identity.node_id, grpc_target=node.advertise))
        assert code_of(e) == grpc.StatusCode.FAILED_PRECONDITION
        with pytest.raises(grpc.RpcError) as e:
            copy(destination=pb.NodeAddress(node_id="x", grpc_target="127.0.0.1:1"))
        assert code_of(e) == grpc.StatusCode.UNAVAILABLE
        assert dst.store.get_record(bid) is None
        flip_byte(blk_path(node, bid))
        with pytest.raises(grpc.RpcError) as e:
            copy()
        assert code_of(e) == grpc.StatusCode.DATA_LOSS
        assert dst.store.get_record(bid) is None
        assert wait_for(lambda: any(r.block_id == bid for r in controller.reports))
        with pytest.raises(grpc.RpcError) as e:
            copy()
        assert code_of(e) == grpc.StatusCode.DATA_LOSS or code_of(e) == grpc.StatusCode.NOT_FOUND
    finally:
        dst.stop(0.5)


def test_copy_destination_without_space(tmp_path, controller, node, stub):
    dst = StorageNode(make_cfg(tmp_path / "n2", controller.target, capacity_bytes=1000)).start()
    try:
        data = os.urandom(50_000)
        bid = new_id()
        put(stub, bid, data)
        with pytest.raises(grpc.RpcError) as e:
            stub.CopyBlock(pb.CopyBlockRequest(
                block_id=bid, destination=pb.NodeAddress(node_id=dst.identity.node_id, grpc_target=dst.advertise),
                expected_storage_sha256=sha(data), stored_size_bytes=len(data)), timeout=15)
        assert code_of(e) == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert dst.store.get_record(bid) is None
        assert get(stub, bid) == data
    finally:
        dst.stop(0.5)


def test_capacity_enforced_and_reported(tmp_path, controller):
    node = StorageNode(make_cfg(tmp_path / "c", controller.target, capacity_bytes=1_000_000)).start()
    try:
        ch = grpc.insecure_channel(node.advertise)
        stub = pb_grpc.StorageServiceStub(ch)
        a, b = new_id(), new_id()
        put(stub, a, os.urandom(700_000))
        u = node.store.usage()
        assert (u.total_bytes, u.used_bytes, u.available_bytes) == (1_000_000, 700_000, 300_000)
        with pytest.raises(grpc.RpcError) as e:
            put(stub, b, os.urandom(700_000))
        assert code_of(e) == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert node.store.usage().used_bytes == 700_000
        assert list(node.store.tmp_dir.iterdir()) == []
        stub.DeleteBlock(pb.DeleteBlockRequest(block_id=a))
        assert node.store.usage().used_bytes == 0
        put(stub, b, os.urandom(700_000))
        put(stub, new_id(), os.urandom(300_000))
        assert node.store.usage().available_bytes == 0
        assert wait_for(lambda: controller.heartbeats and controller.heartbeats[-1].used_bytes == 1_000_000)
        hb = controller.heartbeats[-1]
        assert (hb.total_bytes, hb.used_bytes, hb.available_bytes) == (1_000_000, 1_000_000, 0)
        assert hb.node_id == node.identity.node_id and hb.boot_id == node.link.boot_id
        ch.close()
    finally:
        node.stop(0.5)


def test_concurrent_reservations_never_exceed_capacity(tmp_path, controller):
    node = StorageNode(make_cfg(tmp_path / "cc", controller.target, capacity_bytes=1_000_000)).start()
    try:
        ch = grpc.insecure_channel(node.advertise)
        stub = pb_grpc.StorageServiceStub(ch)
        results = []

        def worker():
            try:
                put(stub, new_id(), os.urandom(300_000))
                results.append("ok")
            except grpc.RpcError as exc:
                results.append(exc.code())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert results.count("ok") == 3
        assert results.count(grpc.StatusCode.RESOURCE_EXHAUSTED) == 5
        assert node.store.usage().used_bytes == 900_000
        ch.close()
    finally:
        node.stop(0.5)


def test_used_bytes_rebuilt_after_restart(tmp_path, controller):
    cfg = make_cfg(tmp_path / "r", controller.target)
    n = StorageNode(cfg).start()
    ch = grpc.insecure_channel(n.advertise)
    stub = pb_grpc.StorageServiceStub(ch)
    ids = {}
    for size in (1000, 25_000, 400_000):
        bid = new_id()
        ids[bid] = os.urandom(size)
        put(stub, bid, ids[bid])
    node_id, boot1 = n.identity.node_id, n.link.boot_id
    ch.close()
    (n.store.tmp_dir / "lixo.part").write_bytes(b"parcial")
    orphan = new_id()
    (n.store.block_dir / orphan[:2]).mkdir(exist_ok=True)
    (n.store.block_dir / orphan[:2] / f"{orphan}.blk").write_bytes(b"sem meta")
    n.stop(0.5)

    n2 = StorageNode(cfg).start()
    try:
        assert n2.identity.node_id == node_id
        assert n2.link.boot_id != boot1
        assert n2.store.usage().used_bytes == sum(len(v) for v in ids.values())
        assert n2.store.usage().blocks == 3
        assert not (n2.store.tmp_dir / "lixo.part").exists()
        assert not (n2.store.block_dir / orphan[:2] / f"{orphan}.blk").exists()
        ch = grpc.insecure_channel(n2.advertise)
        stub = pb_grpc.StorageServiceStub(ch)
        for bid, data in ids.items():
            assert get(stub, bid) == data
        ch.close()
    finally:
        n2.stop(0.5)


def test_register_heartbeat_and_reconcile(tmp_path, controller):
    cfg = make_cfg(tmp_path / "h", controller.target, advertise="storage-9:50052", machine_id="mach-A", disk_id="disk-A")
    n = StorageNode(cfg).start()
    try:
        assert wait_for(lambda: controller.registers and controller.inventories)
        reg = controller.registers[0]
        assert reg.node.node_id == n.identity.node_id
        assert (reg.node.machine_id, reg.node.disk_id, reg.node.grpc_target) == ("mach-A", "disk-A", "storage-9:50052")
        assert reg.boot_id == n.link.boot_id and reg.total_bytes == cfg.capacity_bytes
        assert len(controller.inventories[0]) == 1 and len(controller.inventories[0][0].blocks) == 0
        assert wait_for(lambda: len(controller.heartbeats) >= 2)
        assert all(h.node_id == n.identity.node_id for h in controller.heartbeats)
    finally:
        n.stop(0.5)


def test_reconcile_inventory_and_obsolete_deleted(tmp_path, controller):
    cfg = make_cfg(tmp_path / "rc", controller.target)
    n = StorageNode(cfg).start()
    ch = grpc.insecure_channel(n.advertise)
    stub = pb_grpc.StorageServiceStub(ch)
    keep, stale = new_id(), new_id()
    d_keep, d_stale = os.urandom(5000), os.urandom(6000)
    put(stub, keep, d_keep, version=3)
    put(stub, stale, d_stale)
    ch.close()
    n.stop(0.5)
    controller.inventories.clear()
    controller.obsolete = [stale, new_id()]
    n2 = StorageNode(cfg).start()
    try:
        assert wait_for(lambda: controller.inventories)
        blocks = {b.block_id: b for batch in controller.inventories[-1] for b in batch.blocks}
        assert set(blocks) == {keep, stale}
        assert blocks[keep].storage_sha256 == sha(d_keep) and blocks[keep].version == 3
        assert blocks[keep].stored_size_bytes == len(d_keep)
        assert wait_for(lambda: n2.store.get_record(stale) is None)
        assert n2.store.get_record(keep) is not None
        assert n2.store.usage().used_bytes == len(d_keep)
        assert not blk_path(n2, stale).exists()
    finally:
        n2.stop(0.5)


def test_reregisters_when_controller_forgets_node(tmp_path, controller):
    n = StorageNode(make_cfg(tmp_path / "rr", controller.target)).start()
    try:
        assert wait_for(lambda: len(controller.registers) == 1 and controller.heartbeats)
        controller.fail_heartbeat_not_found = True
        assert wait_for(lambda: len(controller.registers) >= 2, 8)
        controller.fail_heartbeat_not_found = False
        before = len(controller.heartbeats)
        assert wait_for(lambda: len(controller.heartbeats) > before, 8)
    finally:
        n.stop(0.5)


def test_node_works_without_controller(tmp_path):
    n = StorageNode(make_cfg(tmp_path / "nc", "127.0.0.1:1")).start()
    try:
        ch = grpc.insecure_channel(n.advertise)
        stub = pb_grpc.StorageServiceStub(ch)
        data = os.urandom(10_000)
        bid = new_id()
        put(stub, bid, data)
        assert get(stub, bid) == data
        assert not n.link.registered
        ch.close()
    finally:
        n.stop(0.5)


def test_concurrent_distinct_and_same_block(stub, node):
    blobs = {new_id(): os.urandom(100_000 + i) for i in range(12)}
    errors = []

    def worker(bid, data):
        try:
            put(stub, bid, data)
            assert get(stub, bid) == data
        except Exception as exc:
            errors.append(exc)

    ts = [threading.Thread(target=worker, args=kv) for kv in blobs.items()]
    same_id, same_data = new_id(), os.urandom(200_000)
    ts += [threading.Thread(target=worker, args=(same_id, same_data)) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    assert node.store.usage().blocks == 13
    assert node.store.usage().used_bytes == sum(len(v) for v in blobs.values()) + len(same_data)
    assert list(node.store.tmp_dir.iterdir()) == []


def test_corrupt_tool_then_scrub_detects(stub, node, controller, capsys):
    data = os.urandom(90_000)
    bid = new_id()
    put(stub, bid, data)
    events = node.cfg.data_dir / "faults.jsonl"
    rc = corrupt_tool.main(["--data-dir", str(node.cfg.data_dir), "--block-id", bid, "--events-file", str(events)])
    assert rc == 0
    assert events.read_text().count("fault.injected") == 1
    assert node.scrubber.run_pass()["corrupt"] == 1
    assert wait_for(lambda: any(r.block_id == bid for r in controller.reports))
    for mode in ("truncate", "append", "delete"):
        b2 = new_id()
        put(stub, b2, os.urandom(30_000))
        assert corrupt_tool.main(["--data-dir", str(node.cfg.data_dir), "--block-id", b2, "--mode", mode]) == 0
        node.scrubber.run_pass()
        assert wait_for(lambda: any(r.block_id == b2 for r in controller.reports))
    assert corrupt_tool.main(["--data-dir", str(node.cfg.data_dir), "--block-id", "nao-existe"]) == 1


def test_verify_on_read_can_be_disabled_but_copy_still_verifies(tmp_path, controller):
    a = StorageNode(make_cfg(tmp_path / "a", controller.target, verify_on_read=False)).start()
    b = StorageNode(make_cfg(tmp_path / "b", controller.target)).start()
    try:
        ca = grpc.insecure_channel(a.advertise)
        stub = pb_grpc.StorageServiceStub(ca)
        data = os.urandom(70_000)
        bid = new_id()
        put(stub, bid, data)
        flip_byte(blk_path(a, bid))
        assert get(stub, bid) != data
        with pytest.raises(grpc.RpcError) as e:
            stub.CopyBlock(pb.CopyBlockRequest(
                block_id=bid, destination=pb.NodeAddress(node_id="b", grpc_target=b.advertise),
                expected_storage_sha256=sha(data), stored_size_bytes=len(data)), timeout=10)
        assert code_of(e) == grpc.StatusCode.DATA_LOSS
        assert b.store.get_record(bid) is None
        ca.close()
    finally:
        a.stop(0.5)
        b.stop(0.5)
