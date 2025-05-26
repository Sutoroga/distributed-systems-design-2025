import hazelcast
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d-%H-%M-%S'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Connecting to Hazelcast cluster...")
    client = hazelcast.HazelcastClient(
        cluster_name="my-hazelcast-cluster",
        cluster_members=[
            "127.0.0.1:5701",
            "127.0.0.1:5702",
            "127.0.0.1:5703"
        ]
    )

    map_name = "distributed-map"
    distributed_map = client.get_map(map_name).blocking()

    logger.info("Populating distributed map with 1000 entries...")
    for i in range(1000):
        distributed_map.put(str(i), f"{i}")
        if i % 100 == 0:
            logger.info(f"Inserted {i} entries...")

    logger.info("Completed writing to the distributed map.")
    client.shutdown()
    logger.info("Hazelcast client shutdown.")

if __name__ == "__main__":
    main()
