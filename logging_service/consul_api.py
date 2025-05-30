import consul
import socket
import logging
from fastapi import HTTPException
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)
load_dotenv()
CONSUL_HOST = os.getenv("CONSUL_HOST")
CONSUL_PORT = os.getenv("CONSUL_PORT")

_consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

def register_service(service_name: str, service_port: int, grpc: bool = False):
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        service_id = f"{service_name}-{hostname}"

        c = _consul_client

        check = None
        if grpc:
            check = {
                "grpc": f"{ip_address}:{service_port}",
                "grpc_use_tls": False,
                "interval": "10s",
            }
            logger.info(f"Using gRPC health check with IP for {service_name}")
        else:
            check = {
                "http": f"http://{ip_address}:{service_port}/health",
                "interval": "10s",
            }
            logger.info(f"Using HTTP health check with IP for {service_name}")

        c.agent.service.register(
            service_name,
            service_id,
            address=ip_address,
            port=service_port,
            check=check
        )

        logger.info(f"Registered {service_name} with Consul at {ip_address}:{service_port} with ID: {service_id}")
    except Exception as e:
        logger.error(f"Consul registration failed: {e}")

def get_service_addresses(service_name: str) -> list[str]:
    """
    Queries Consul for healthy instances of a given service and returns their addresses.
    """
    try:
        index, data = _consul_client.health.service(service_name, passing=True)
        addresses = [f"{entry['Service']['Address']}:{entry['Service']['Port']}" for entry in data]
        if not addresses:
            raise ValueError(f"No healthy instances found for {service_name}")
        logger.info(f"Found healthy instances for {service_name}: {addresses}")
        return addresses
    except consul.ConsulException as e:
        logger.error(f"Error querying Consul for {service_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get service info from Consul: {e}")

def get_value_from_consul_kv(path: str) -> str:
    """
    Fetches a single value from Consul's Key/Value store.
    """
    try:
        index, data = _consul_client.kv.get(path)
        if not data or 'Value' not in data:
            raise ValueError(f"No data found at Consul KV path: {path}")
        value = data['Value'].decode('utf-8').strip()
        logger.info(f"Fetched from Consul KV path '{path}': '{value}'")
        return value
    except consul.ConsulException as e:
        logger.error(f"Error fetching from Consul KV path '{path}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get config from Consul: {e}")

def get_list_from_consul_kv(path: str) -> list[str]:
    """
    Fetches a list of values from Consul's Key/Value store.
    Assumes the value is a comma-separated string.
    """
    try:
        index, data = _consul_client.kv.get(path)
        if not data or 'Value' not in data:
            raise ValueError(f"No data found at Consul KV path: {path}")
        value_str = data['Value'].decode('utf-8')
        values = [v.strip() for v in value_str.split(',')]
        logger.info(f"Fetched list from Consul KV path '{path}': {values}")
        return values
    except consul.ConsulException as e:
        logger.error(f"Error fetching list from Consul KV path '{path}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get config list from Consul: {e}")