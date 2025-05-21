import hazelcast
import logging
import multiprocessing
import time


logging.getLogger("hazelcast").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d-%H-%M-%S"
)
logger = logging.getLogger(__name__)

def increment_worker(client_id):
    client = hazelcast.HazelcastClient(
        cluster_name="my-hazelcast-cluster",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
    )
    my_map = client.get_map("race-map").blocking()

    logger.info(f"[Client-{client_id}] Starting...")
    my_map.put_if_absent("key", 0)

    for _ in range(10_000):
        value = my_map.get("key")
        my_map.put("key", value + 1)

    logger.info(f"[Client-{client_id}] Finished.")
    client.shutdown()


def main():
    start_time = time.time()
    logger.info("Launching 3 clients for concurrent map increment...")

    processes = []
    for i in range(3):
        p = multiprocessing.Process(target=increment_worker, args=(i + 1,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    client = hazelcast.HazelcastClient(
        cluster_name="my-hazelcast-cluster",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
    )
    final_value = client.get_map("race-map").blocking().get("key")
    logger.info(f"Final value for 'key': {final_value}")
    logger.info(f"Total duration: {round(time.time() - start_time, 2)} seconds")
    client.shutdown()


if __name__ == "__main__":
    main()
