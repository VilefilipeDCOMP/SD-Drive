from dataclasses import replace
from uuid import uuid4

import grpc
import pytest

import dcomp_drive_pb2 as pb
from common.ports import NodeSnapshot
from metadata.service import AllocationError, MetadataService
from common.simulated_registry import SimulatedNodeRegistry

def nodes(count=3):
    return [NodeSnapshot(str(uuid4()), f'maquina-{i}', f'disco-{i}', f'storage-{i}:50052', pb.NODE_ALIVE, 1000, 0, 1000)
            for i in range(count)]

def request(size=100, **kwargs):
    return pb.AllocateUploadRequest(path='/teste.txt', operation_id=str(uuid4()), plaintext_size_bytes=size, plaintext_sha256=b'a' * 32, encryption=pb.ENCRYPTION_NONE, chunks=[pb.ChunkManifest(block_id=str(uuid4()), index=0, stored_size_bytes=size, storage_sha256=b'b' * 32)], **kwargs)

def service(items=None, **kwargs):
    return MetadataService(SimulatedNodeRegistry(nodes() if items is None else items), **kwargs)

def assert_error(code, function, *args):
    with pytest.raises(AllocationError) as exc:
        function(*args)

    assert exc.value.code == code

def test_registry_accepts_reference_interface_identifiers():
    items = nodes()
    registry = SimulatedNodeRegistry(items)
    node = items[0]

    assert registry.get_node_state(node_id=node.node_id) == pb.NODE_ALIVE
    assert registry.get_node_capacity(node_id=node.node_id) == node
    assert node in registry.list_eligible_nodes()

def test_allocates_three_distinct_destinations_and_fixed_retry():
    items = nodes()
    metadata = service(items)
    req = request()
    first = metadata.allocate_upload(req)

    assert first.version == 1
    assert first.replication_factor == first.write_quorum == 3
    assert {node.node_id for node in first.chunks[0].targets} == {node.node_id for node in items}
    assert {
        (node.node_id, node.machine_id, node.disk_id, node.grpc_target)
        for node in first.chunks[0].targets
    } == {
        (node.node_id, node.machine_id, node.disk_id, node.grpc_target)
        for node in items
    }
    assert first.lease_expires_at_unix_ms > 0
    assert metadata.allocate_upload(req) == first

    first.ClearField("chunks")
    assert len(metadata.allocate_upload(req).chunks[0].targets) == 3

def test_grpc_entry_preserves_protobuf_response():
    req = request()
    metadata = service()

    response = metadata.AllocateUpload(req, None)
    decoded_response = pb.AllocateUploadResponse.FromString(response.SerializeToString())

    assert decoded_response == metadata.allocate_upload(req)
    assert len(decoded_response.chunks[0].targets) == 3

@pytest.mark.parametrize('change', ["manifesto", 'exclusions', "path"])
def test_same_operation_cannot_change_request(change):
    metadata = service()
    req = request()
    metadata.allocate_upload(req)

    if change == "manifesto":
        req.chunks[0].storage_sha256 = b"c" * 32
    elif change == 'exclusions':
        req.excluded_node_ids.append(str(uuid4()))
    else:
        req.path = "/outro.txt"

    assert_error(grpc.StatusCode.INVALID_ARGUMENT, metadata.allocate_upload, req)

@pytest.mark.parametrize('state', [pb.NODE_SUSPECT, pb.NODE_DEAD, pb.NODE_DRAINING])
def test_filters_ineligible_nodes_and_explicit_exclusions(state):
    items = nodes(5)
    items[0] = replace(items[0], state=state)
    req = request(excluded_node_ids=[items[1].node_id])

    result = service(items).allocate_upload(req)

    assert {node.node_id for node in result.chunks[0].targets} == {node.node_id for node in items[2:]}

@pytest.mark.parametrize('field', ['machine_id', 'disk_id'])
def test_rejects_repeated_failure_domains(field):
    items = nodes()
    items[1] = replace(items[1], **{field: getattr(items[0], field)})

    assert_error(grpc.StatusCode.FAILED_PRECONDITION, service(items).allocate_upload, request())

def test_finds_valid_combination_when_first_candidate_blocks_it():
    items = nodes(4)
    items[1] = replace(items[1], machine_id=items[0].machine_id)
    items[2] = replace(items[2], disk_id=items[0].disk_id)

    result = service(items).allocate_upload(request())

    assert {node.node_id for node in result.chunks[0].targets} == {node.node_id for node in items[1:]}

@pytest.mark.parametrize('used,available,size,ok', [(800, 200, 100, True), (801, 199, 100, False), (0, 99, 100, False)])
def test_capacity_after_allocation(used, available, size, ok):
    items = [replace(node, used_bytes=used, available_bytes=available) for node in nodes()]
    metadata = service(items)

    if ok:
        assert metadata.allocate_upload(request(size)).upload_id
    else:
        assert_error(grpc.StatusCode.FAILED_PRECONDITION, metadata.allocate_upload, request(size))

def test_reserves_capacity_between_uploads():
    metadata = service()
    metadata.allocate_upload(request(500))

    second = request(500)
    second.path = "/outro.txt"

    assert_error(grpc.StatusCode.FAILED_PRECONDITION, metadata.allocate_upload, second)

def test_failed_multiblock_allocation_does_not_leak_reservations():
    metadata = service()
    req = request(1000)
    req.chunks[0].stored_size_bytes = 500
    req.chunks.append(pb.ChunkManifest(block_id=str(uuid4()), index=1, stored_size_bytes=500, storage_sha256=b'x' * 32))

    assert_error(grpc.StatusCode.FAILED_PRECONDITION, metadata.allocate_upload, req)
    assert metadata.allocate_upload(request(900)).upload_id

def test_path_is_exclusive_and_expired_operation_is_not_revived():
    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    req = request()
    metadata.allocate_upload(req)

    assert_error(grpc.StatusCode.ABORTED, metadata.allocate_upload, request())

    now[0] += 10

    assert_error(grpc.StatusCode.ABORTED, metadata.allocate_upload, req)
    assert metadata.allocate_upload(request()).upload_id

@pytest.mark.parametrize("replication_factor,write_quorum", [(1, "all"), (2, "all"), (3, 2)])
def test_effective_quorum(replication_factor, write_quorum):
    result = service(replication_factor=replication_factor, write_quorum=write_quorum).allocate_upload(request())

    assert result.replication_factor == replication_factor
    assert result.write_quorum == (replication_factor if write_quorum == "all" else write_quorum)

@pytest.mark.parametrize("replication_factor,write_quorum", [(0, "all"), (3, 0), (2, 3), (3, True)])
def test_invalid_quorum_is_rejected(replication_factor, write_quorum):
    with pytest.raises(ValueError):
        service(replication_factor=replication_factor, write_quorum=write_quorum)

@pytest.mark.parametrize('field,value', [('path', '../teste'), ('path', '/a/../teste'), ('path', '/'), ('operation_id', 'invalido'), ('plaintext_sha256', b'curto'), ('encryption', pb.ENCRYPTION_MODE_UNSPECIFIED)])
def test_rejects_invalid_manifest(field, value):
    req = request()
    setattr(req, field, value)

    assert_error(grpc.StatusCode.INVALID_ARGUMENT, service().allocate_upload, req)

def test_grpc_adapter_translates_domain_error():
    class Context:
        def abort(self, code, details):
            raise AllocationError(code, details)

    assert_error(grpc.StatusCode.FAILED_PRECONDITION, service([]).AllocateUpload, request(), Context())

def test_concurrent_uploads_cannot_lock_same_path():
    from concurrent.futures import ThreadPoolExecutor

    metadata = service()

    def allocate(_):
        try:
            metadata.allocate_upload(request())
            return "alocado"
        except AllocationError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(allocate, range(8)))

    assert results.count("alocado") == 1
    assert results.count(grpc.StatusCode.ABORTED) == 7

def test_exclusions_are_a_set_for_idempotency():
    metadata = service()
    excluded = [str(uuid4()), str(uuid4())]
    req = request(excluded_node_ids=excluded)
    first = metadata.allocate_upload(req)

    del req.excluded_node_ids[:]
    req.excluded_node_ids.extend([excluded[1], excluded[0], excluded[0]])

    assert metadata.allocate_upload(req) == first

@pytest.mark.parametrize('change', ["hash", "size", "index", "block_id", "nonce"])
def test_rejects_invalid_chunk(change):
    req = request()
    chunk = req.chunks[0]

    if change == "hash":
        chunk.storage_sha256 = b"curto"
    elif change == "size":
        chunk.stored_size_bytes = 99
    elif change == "index":
        chunk.index = 1
    elif change == "block_id":
        chunk.block_id = "invalido"
    else:
        chunk.nonce = b"nonce-sem-cifra"

    assert_error(grpc.StatusCode.INVALID_ARGUMENT, service().allocate_upload, req)

def test_encrypted_manifest_accounts_for_gcm_tag():
    req = request()
    req.encryption = pb.ENCRYPTION_AES_256_GCM
    req.chunks[0].nonce = b"n" * 12
    req.chunks[0].stored_size_bytes = 116

    assert service().allocate_upload(req).upload_id

def test_nonexistent_parent_is_not_implicitly_created():
    req = request()
    req.path = "/pasta/teste.txt"

    assert_error(grpc.StatusCode.NOT_FOUND, service().allocate_upload, req)

def test_expired_upload_releases_capacity_for_same_path():
    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    expired = request(900)
    metadata.allocate_upload(expired)

    now[0] += 10
    replacement = metadata.allocate_upload(request(900))

    assert replacement.upload_id
    assert_error(grpc.StatusCode.ABORTED, metadata.allocate_upload, expired)

def test_expiration_releases_all_chunks_of_upload():
    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    expired = request(800)
    expired.chunks[0].stored_size_bytes = 400
    expired.chunks.append(pb.ChunkManifest(block_id=str(uuid4()), index=1, stored_size_bytes=400, storage_sha256=b'c' * 32))
    metadata.allocate_upload(expired)

    now[0] += 10

    assert metadata.allocate_upload(request(900)).upload_id

def test_expiration_preserves_capacity_reserved_by_active_upload():
    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    metadata.allocate_upload(request(300))

    now[0] += 5
    active = request(300)
    active.path = "/ativo.txt"
    previous = metadata.allocate_upload(active)

    now[0] += 5
    metadata.allocate_upload(request(600))

    assert metadata.allocate_upload(active) == previous
    overflow = request(1)
    overflow.path = "/sem_espaco.txt"
    assert_error(grpc.StatusCode.FAILED_PRECONDITION, metadata.allocate_upload, overflow)

def test_expired_retry_does_not_release_new_upload_reservations():
    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    expired = request(900)
    metadata.allocate_upload(expired)

    now[0] += 10
    active = request(900)
    previous = metadata.allocate_upload(active)

    for _ in range(2):
        assert_error(grpc.StatusCode.ABORTED, metadata.allocate_upload, expired)

    assert metadata.allocate_upload(active) == previous
    assert_error(grpc.StatusCode.ABORTED, metadata.allocate_upload, request(1))

    overflow = request(1)
    overflow.path = "/outro.txt"
    assert_error(grpc.StatusCode.FAILED_PRECONDITION, metadata.allocate_upload, overflow)

def test_expiration_does_not_reuse_block_ids():
    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    expired = request(100)
    metadata.allocate_upload(expired)

    now[0] += 10
    retry = request(100)
    retry.chunks[0].block_id = expired.chunks[0].block_id

    assert_error(grpc.StatusCode.INVALID_ARGUMENT, metadata.allocate_upload, retry)

def test_concurrent_allocations_release_expired_reservation_only_once():
    from concurrent.futures import ThreadPoolExecutor

    now = [1000.0]
    metadata = service(clock=lambda: now[0], lease_seconds=10)
    metadata.allocate_upload(request(900))
    now[0] += 10

    def allocate(index):
        req = request(900)
        req.path = f"/arquivo-{index}.txt"
        try:
            metadata.allocate_upload(req)
            return "alocado"
        except AllocationError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(allocate, range(8)))

    assert results.count("alocado") == 1
    assert results.count(grpc.StatusCode.FAILED_PRECONDITION) == 7
