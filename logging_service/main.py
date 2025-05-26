import grpc
from concurrent import futures
import logging
import time

from logservice_protocol import log_pb2, log_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Logging-Service (gRPC) starting up...")

logs = {}  # UUID: message
processed_ids = set()


# gRPC service implementation
class LogServiceServicer(log_pb2_grpc.LogServiceServicer):

    def LogMessage(self, request, context):
        request_id = request.id

        if request_id in processed_ids:
            logger.warning(f"Duplicate message detected: ID {request_id}")
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Duplicate message detected")

        logs[request_id] = request.msg
        processed_ids.add(request_id)

        logger.info(f"Stored message (ID: {request_id}): \"{request.msg}\"")
        return log_pb2.LogResponse(status="Message logged", id=request_id)

    def GetMessages(self, request, context):
        logger.info("Returned all stored messages")
        return log_pb2.Messages(messages=list(logs.values()))

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

if __name__ == "__main__":
    serve()
