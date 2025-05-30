import grpc
from concurrent import futures
import logging
import time

import hazelcast
from logservice_protocol import log_pb2, log_pb2_grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from consul_api import register_service, get_value_from_consul_kv, get_list_from_consul_kv
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Logging-Service (gRPC + Hazelcast + gRPC Health) starting up...")

hazelcast_members = []

try:
    hazelcast_members = get_list_from_consul_kv("hazelcast/members")
    logger.info(f"Using Hazelcast nodes from Consul: {hazelcast_members}")
except HTTPException as e:
    logger.error(f"Failed to get Hazelcast members from Consul: {e.detail}")
    import sys
    sys.exit(1)
except Exception as e:
    logger.error(f"An unexpected error occurred while fetching Hazelcast members: {e}")
    import sys
    sys.exit(1)

try:
    hazelcast_cluster_name = get_value_from_consul_kv("hazelcast/cluster_name")
    logger.info(f"Using Hazelcast cluster name from Consul: '{hazelcast_cluster_name}'")
except HTTPException as e:
    logger.warning(f"Failed to get Hazelcast cluster name from Consul, using default: '{hazelcast_cluster_name}': {e.detail}")

except Exception as e:
    logger.warning(f"Failed to get Hazelcast cluster name from Consul, using default: '{hazelcast_cluster_name}': {e}")

try:
    hz_client = hazelcast.HazelcastClient(
        cluster_name=hazelcast_cluster_name,
        cluster_members=hazelcast_members
    )
    logger.info(f"Connected to Hazelcast cluster '{hazelcast_cluster_name}' using nodes: {hazelcast_members}")
except Exception as e:
    logger.error(f"Failed to connect to Hazelcast: {e}")

# Distributed log storage map
if 'hz_client' in locals():
    logs_map = hz_client.get_map("logging_service_logs").blocking()
else:
    logs_map = None
    logger.warning("Hazelcast client not initialized, log storage will be unavailable.")

# gRPC LogService implementation
class LogServiceServicer(log_pb2_grpc.LogServiceServicer):
    def LogMessage(self, request, context):
        request_id = request.id
        if logs_map and logs_map.contains_key(request_id):
            logger.warning(f"Duplicate message detected: ID {request_id}")
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Duplicate message detected")
            return

        if logs_map:
            logs_map.put(request_id, request.msg)
            logger.info(f"Stored message (ID: {request_id}): \"{request.msg}\"")
            return log_pb2.LogResponse(status="Message logged", id=request_id)
        else:
            logger.warning("Cannot store log message, Hazelcast not initialized.")
            context.abort(grpc.StatusCode.UNAVAILABLE, "Log storage unavailable")
            return

    def GetMessages(self, request, context):
        if logs_map:
            values = logs_map.values()
            logger.info("Returned all stored messages")
            return log_pb2.Messages(messages=list(values))
        else:
            logger.warning("Cannot retrieve log messages, Hazelcast not initialized.")
            context.abort(grpc.StatusCode.UNAVAILABLE, "Log storage unavailable")
            return



def serve():
    grpc_port = 8001
    # Register instance in Consul
    register_service("logging-service", grpc_port, grpc=True)

    # Start gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    log_pb2_grpc.add_LogServiceServicer_to_server(LogServiceServicer(), server)

    # Health check registration
    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

    server.add_insecure_port(f'[::]:{grpc_port}')
    logger.info(f"Logging-Service running on gRPC port {grpc_port}")
    server.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info("Logging-Service shutting down")
        server.stop(0)
        if 'hz_client' in locals():
            hz_client.shutdown()


if __name__ == "__main__":
    serve()