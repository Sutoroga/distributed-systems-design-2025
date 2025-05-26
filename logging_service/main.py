import grpc
from concurrent import futures
import logging
import time
import os

import hazelcast
from logservice_protocol import log_pb2, log_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Logging-Service (gRPC + Hazelcast) starting up...")

cluster_name = os.getenv("CLUSTER_NAME", "my-hazelcast-cluster")
hazelcast_node = os.getenv("HAZELCAST_NODE", "127.0.0.1:5701")

hz_client = hazelcast.HazelcastClient(
    cluster_name=cluster_name,
    cluster_members=[hazelcast_node]
)

# Distributed map for log storage
logs_map = hz_client.get_map("logging_service_logs").blocking()

class LogServiceServicer(log_pb2_grpc.LogServiceServicer):

    def LogMessage(self, request, context):
        request_id = request.id

        if logs_map.contains_key(request_id):
            logger.warning(f"Duplicate message detected: ID {request_id}")
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Duplicate message detected")

        logs_map.put(request_id, request.msg)
        logger.info(f"Stored message (ID: {request_id}): \"{request.msg}\"")
        return log_pb2.LogResponse(status="Message logged", id=request_id)

    def GetMessages(self, request, context):
        values = logs_map.values()
        logger.info("Returned all stored messages")
        return log_pb2.Messages(messages=list(values))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    log_pb2_grpc.add_LogServiceServicer_to_server(LogServiceServicer(), server)
    server.add_insecure_port('[::]:8001')
    logger.info("Logging-Service running on port 8001")
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        logger.info("Logging-Service shutting down")
        server.stop(0)
        hz_client.shutdown()

if __name__ == "__main__":
    serve()
