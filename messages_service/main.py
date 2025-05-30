from confluent_kafka import Consumer
import threading
import os
import logging

from fastapi import FastAPI
import uvicorn
from consul_api import register_service, get_value_from_consul_kv
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Messages-Service starting up...")

load_dotenv()

register_service("messages-service", 8002)
app = FastAPI()

kafka_bootstrap_servers = None
kafka_topic = None
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
messages = []

def consume_from_kafka():
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([kafka_topic])

    logger.info("Kafka consumer started.")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning(f"Kafka error: {msg.error()}")
            continue
        value = msg.value().decode("utf-8")
        logger.info(f"Consumed from Kafka: {value}")
        messages.append(value)


@app.get("/message/")
def get_message():
    logger.info("Handled GET request at /message/")
    return {"messages": messages}


@app.on_event("startup")
async def startup_event():
    global kafka_bootstrap_servers, kafka_topic
    try:
        kafka_bootstrap_servers = get_value_from_consul_kv("kafka/bootstrap_servers")
        logger.info(f"Kafka Bootstrap Servers from Consul: {kafka_bootstrap_servers}")
        kafka_topic = get_value_from_consul_kv("kafka/topic")
        logger.info(f"Kafka Topic from Consul: {kafka_topic}")
        # Start consumer thread after fetching config
        t = threading.Thread(target=consume_from_kafka, daemon=True)
        t.start()
    except HTTPException as e:
        logger.error(f"Failed to load Kafka config from Consul: {e.detail}")

    except Exception as e:
        logger.error(f"An unexpected error occurred while loading Kafka config: {e}")


@app.get("/health")
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    logger.info("Messages-Service running on port 8002")
    uvicorn.run(app, host="0.0.0.0", port=8002)