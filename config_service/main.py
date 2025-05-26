from fastapi import FastAPI, HTTPException, Request
import json
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)
logger.info("Config-Service starting up...")

app = FastAPI()

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")

@app.get("/services/{service_name}")
def get_service_instances(request: Request, service_name: str):
    logger.info(f"Host {request.client.host} asks for {service_name}")
    try:
        with open(CONFIG_FILE, "r") as f:
            registry = json.load(f)

        service_list = registry.get(service_name, [])
        logger.info(f"Found {len(service_list)} services for {service_name}: {service_list}")
        return service_list
    except Exception as e:
        logger.error(f"Failed to load config.json {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get service data.")
