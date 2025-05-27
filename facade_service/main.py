from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import grpc
import os
import uuid
import requests
import time
import random
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from logservice_protocol import log_pb2, log_pb2_grpc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Facade-Service starting up...")

# Load .env
load_dotenv()

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL")

# Retry settings
MAX_RETRY_WINDOW = 60
RETRY_INTERVAL = 2

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
_kafka_producer = None
app = FastAPI()

class MessageRequest(BaseModel):
    msg: str


def get_service_addresses(service_name: str):
    try:
        response = requests.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        response.raise_for_status()
        addresses = response.json()
        if not addresses:
            raise ValueError(f"No instances found for {service_name}")
        return addresses
    except Exception as e:
        logger.error(f"Error fetching service instances from config-server: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get service info: {str(e)}")


def send_log_message(id: str, msg: str, target: str):
    try:
        channel = grpc.insecure_channel(target)
        stub = log_pb2_grpc.LogServiceStub(channel)
        response = stub.LogMessage(log_pb2.LogRequest(id=id, msg=msg), timeout=5)
        logger.info(f"Message sent to {target}")
        return response
    except grpc.RpcError as e:
        logger.warning(f"gRPC error with {target}: {e.code().name} - {e.details()}")
        if e.code() == grpc.StatusCode.ALREADY_EXISTS:
            return log_pb2.LogResponse(status="Duplicate", id=id)
        raise

def ensure_kafka_topic_exists():
    a = AdminClient({'bootstrap.servers': KAFKA_SERVERS})

    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=3) for topic in [KAFKA_TOPIC]]
    fs = a.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()
            logger.info("Topic {} created".format(topic))
        except Exception as e:
            logger.error("Failed to create topic {}: {}".format(topic, e))

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        logger.info("Initializing Kafka producer...")
        _kafka_producer = Producer({"bootstrap.servers": KAFKA_SERVERS})
    return _kafka_producer

def produce_to_kafka(message_id: str, msg: str):
    payload = msg
    deadline = time.time() + 30  # retry window
    attempt = 1

    while time.time() < deadline:
        try:
            producer = get_kafka_producer()
            producer.produce(KAFKA_TOPIC, value=payload)
            producer.flush()
            logger.info(f"[Kafka] Message enqueued (attempt {attempt}): {payload}")
            return
        except Exception as e:
            logger.warning(f"[Kafka] Attempt {attempt} failed: {e}")
            time.sleep(2)
            attempt += 1

    logger.error(f"[Kafka] All retry attempts failed for: {payload}")
    raise Exception("Kafka not available after retries")

# Create Kafka topic at init
ensure_kafka_topic_exists()

@app.post("/facade-service")
def send_message(request: MessageRequest):
    message_id = str(uuid.uuid4())
    logger.info(f"Received POST: \"{request.msg}\" → UUID: {message_id}")

    try:
        produce_to_kafka(message_id, request.msg)
    except Exception as e:
        logger.error(f"Kafka enqueue failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to enqueue message to Kafka")

    logging_services = get_service_addresses("logging-service")
    random.shuffle(logging_services)

    deadline = time.time() + MAX_RETRY_WINDOW
    for target in logging_services:
        while time.time() < deadline:
            try:
                logger.info(f"Selected {target}, trying to send message...")
                response = send_log_message(message_id, request.msg, target)
                return {"status": response.status, "id": response.id}
            except grpc.RpcError:
                logger.info(f"Send message failed, retrying different logging service if available...")
                break
        else:
            continue
        break

    logger.error("All logging-service instances failed or retry window expired.")
    raise HTTPException(status_code=504, detail="Logging services unavailable")


@app.get("/facade-service")
def fetch_combined_response():
    logger.info("Received GET request to /facade-service")

    # Fetch logs from logging-service
    logging_services = get_service_addresses("logging-service")
    random.shuffle(logging_services)

    logs = "Unavailable"
    for target in logging_services:
        try:
            channel = grpc.insecure_channel(target)
            stub = log_pb2_grpc.LogServiceStub(channel)
            response = stub.GetMessages(log_pb2.Empty(), timeout=5)
            logs = "\n".join(response.messages)
            logger.info(f"Retrieved logs from {target}")
            break
        except grpc.RpcError as e:
            logger.warning(f"Failed to fetch logs from {target}: {e.code().name}")
            continue

    messages_service = get_service_addresses("messages-service")
    random.shuffle(messages_service)

    messages = "Unavailable"
    for target in messages_service:
        try:
            msg_response = requests.get(f"http://{target}/message/")
            msg_response.raise_for_status()
            messages = msg_response.text
            logger.info(f"Retrieved messages from {target}")
            break
        except grpc.RpcError as e:
            logger.warning(f"Failed to fetch messages from {target}: {e.code().name}")
            continue

    return {"response": (logs + " " + messages).strip()}


if __name__ == "__main__":
    logger.info("Facade-Service running on port 8000")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

