from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import grpc
import os
import uuid
import requests
import time
from dotenv import load_dotenv
import logging

from logservice_protocol import log_pb2, log_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Facade-Service starting up...")

load_dotenv()

LOGGING_SERVICE_HOST = os.getenv("LOGGING_SERVICE_HOST", "logging_service:8001")
MESSAGES_SERVICE_URL = os.getenv("MESSAGES_SERVICE_URL", "http://messages_service:8002")

app = FastAPI()

# gRPC channel and stub
channel = grpc.insecure_channel(LOGGING_SERVICE_HOST)
log_stub = log_pb2_grpc.LogServiceStub(channel)

# Retry configuration
MAX_RETRY_WINDOW = 60  # retry window
RETRY_INTERVAL = 2     # retry delay

class MessageRequest(BaseModel):
    msg: str

def send_log_with_retry(stub, id: str, msg: str):
    deadline = time.time() + MAX_RETRY_WINDOW
    attempt = 1

    while time.time() < deadline:
        try:
            response = stub.LogMessage(log_pb2.LogRequest(id=id, msg=msg))
            logger.info(f"Message sent successfully to Logging-Service on attempt {attempt}")
            return response
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                logger.warning(f"Message already delivered (deduplication detected) on attempt {attempt}")
                return log_pb2.LogResponse(status="Duplicate", id=id)

            logger.warning(f"gRPC error (attempt {attempt}): {e.code().name} - {e.details()}")
            time.sleep(RETRY_INTERVAL)
            attempt += 1

    logger.error(f"Retry window of {MAX_RETRY_WINDOW} seconds exhausted. Message not sent.")
    raise HTTPException(status_code=504, detail="Logging service unavailable after retries")

@app.post("/facade-service")
def send_message(request: MessageRequest):
    message_id = str(uuid.uuid4())
    logger.info(f"Received POST request with message: \"{request.msg}\". Generated UUID: {message_id}")

    try:
        response = send_log_with_retry(log_stub, message_id, request.msg)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unhandled error during gRPC retry: {e}")
        raise HTTPException(status_code=500, detail="Internal error")

    return {"status": response.status, "id": response.id}

@app.get("/facade-service")
def fetch_combined_response():
    logger.info("Received GET request to /facade_service")

    try:
        grpc_response = log_stub.GetMessages(log_pb2.Empty())
        logs = "\n".join(grpc_response.messages)
        logger.info("Successfully retrieved logs from Logging-Service via gRPC")
    except grpc.RpcError as e:
        logs = f"Logging service error: {e.details()}"
        logger.error(logs)

    try:
        msg_response = requests.get(f"{MESSAGES_SERVICE_URL}/message/")
        msg_response.raise_for_status()
        message = msg_response.text
        logger.info("Successfully retrieved static message from Messages-Service")
    except requests.RequestException as e:
        message = f"Messages service error: {str(e)}"
        logger.error(message)

    combined_response = logs + " " + message
    logger.info("Returning combined response to client")
    return {"response": combined_response.strip()}

if __name__ == "__main__":
    logger.info("Facade-Service running on port 8000")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
