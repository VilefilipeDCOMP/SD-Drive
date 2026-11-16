from hashlib import sha256
from uuid import uuid4

from google.protobuf.json_format import MessageToJson
import dcomp_drive_pb2 as pb
from common.ports import NodeSnapshot
from common.simulated_registry import SimulatedNodeRegistry
from metadata.service import MetadataService

def main():
    registry = SimulatedNodeRegistry([
        NodeSnapshot(str(uuid4()), f"maquina-{i}", f"disco-{i}", f"storage-{i}:50052", pb.NODE_ALIVE, 10 * 1024**3, 0, 10 * 1024**3)
        for i in range(1, 4)
    ])
    metadata = MetadataService(registry)

    data = b"Primeiro upload do SD-Drive."
    request = pb.AllocateUploadRequest(path='/exemplo.txt', operation_id=str(uuid4()), plaintext_size_bytes=len(data), plaintext_sha256=sha256(data).digest(), encryption=pb.ENCRYPTION_NONE, chunks=[pb.ChunkManifest(block_id=str(uuid4()), index=0, stored_size_bytes=len(data), storage_sha256=sha256(data).digest())])

    first = metadata.allocate_upload(request)
    print(MessageToJson(first, preserving_proto_field_name=True))
    print("Nova chamada mantém a mesma alocação:", metadata.allocate_upload(request) == first)

if __name__ == "__main__":
    main()
