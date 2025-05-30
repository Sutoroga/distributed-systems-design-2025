from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import grpc
import uuid
import requests
import time
import random
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from logservice_protocol import log_pb2, log_pb2_grpc

from consul_api import register_service, get_service_addresses, get_value_from_consul_kv


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Facade-Service starting up...")


load_dotenv()

# Retry settings
MAX_RETRY_WINDOW = 60
RETRY_INTERVAL = 2
# Kafka variables to be filled with values after 1st kafka usage
kafka_bootstrap_servers = None
kafka_topic = None
_kafka_producer = None

register_service("facade-service", 8000)
app = FastAPI()

class MessageRequest(BaseModel):
    msg: str

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
    global kafka_bootstrap_servers, kafka_topic
    if kafka_bootstrap_servers and kafka_topic:
        a = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
        new_topics = [NewTopic(kafka_topic, num_partitions=3, replication_factor=3)]
        fs = a.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Topic {topic} created")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
    else:
        logger.warning("Kafka bootstrap servers or topic not configured, skipping topic creation.")

def get_kafka_producer():
    global _kafka_producer, kafka_bootstrap_servers
    if _kafka_producer is None and kafka_bootstrap_servers:
        logger.info("Initializing Kafka producer...")
        _kafka_producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    elif _kafka_producer is None:
        logger.warning("Kafka bootstrap servers not configured, producer not initialized.")
    return _kafka_producer

def produce_to_kafka(message_id: str, msg: str):
    global kafka_topic
    if not kafka_bootstrap_servers or not kafka_topic:
        logger.warning("Kafka bootstrap servers or topic not configured, cannot produce.")
        return

    payload = msg
    deadline = time.time() + 30  # retry window
    attempt = 1

    while time.time() < deadline:
        try:
            producer = get_kafka_producer()
            if producer:
                producer.produce(kafka_topic, value=payload)
                producer.flush()
                logger.info(f"[Kafka] Message enqueued (attempt {attempt}): {payload}")
                return
            else:
                logger.warning("[Kafka] Producer not initialized, cannot send message.")
                time.sleep(2)
                attempt += 1
        except Exception as e:
            logger.warning(f"[Kafka] Attempt {attempt} failed: {e}")
            time.sleep(2)
            attempt += 1

    logger.error(f"[Kafka] All retry attempts failed for: {payload}")
    raise Exception("Kafka not available after retries")

@app.on_event("startup")
async def startup_event():
    global kafka_bootstrap_servers, kafka_topic
    try:
        kafka_bootstrap_servers = get_value_from_consul_kv("kafka/bootstrap_servers")
        logger.info(f"Kafka Bootstrap Servers from Consul: {kafka_bootstrap_servers}")
        kafka_topic = get_value_from_consul_kv("kafka/topic")
        logger.info(f"Kafka Topic from Consul: {kafka_topic}")
        ensure_kafka_topic_exists()
    except HTTPException as e:
        logger.error(f"Failed to load Kafka config from Consul: {e.detail}")

    except Exception as e:
        logger.error(f"An unexpected error occurred while loading Kafka config: {e}")


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

    messages_services = get_service_addresses("messages-service")
    random.shuffle(messages_services)

    messages = "Unavailable"
    for target in messages_services:
        try:
            msg_response = requests.get(f"http://{target}/message/")
            msg_response.raise_for_status()
            messages = msg_response.text
            logger.info(f"Retrieved messages from {target}")
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch messages from {target}: {e}")
            continue

    return {"response": (logs + " " + messages).strip()}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    logger.info("Facade-Service running on port 8000")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)