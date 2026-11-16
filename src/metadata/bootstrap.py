import dcomp_drive_pb2_grpc as rpc
from metadata.service import MetadataService

def create_metadata(registry, config):
    cluster = config.get("cluster", {})
    controller = config.get("controller", {})
    return MetadataService(registry, replication_factor=cluster.get("replication_factor", 3), write_quorum=cluster.get("write_quorum", "all"), max_usage_fraction=cluster.get("max_disk_usage_fraction", 0.90), lease_seconds=controller.get("upload_lease_seconds", 900), max_lifetime_seconds=controller.get("upload_max_lifetime_seconds", 14400), db_path=controller.get("metadata_db_path", "/data/metadata.db"))

def register_metadata(server, registry, config):
    service = create_metadata(registry, config)
    rpc.add_MetadataServiceServicer_to_server(service, server)
    return service
