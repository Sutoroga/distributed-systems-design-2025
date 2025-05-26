import hazelcast
import logging
import multiprocessing
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d-%H-%M-%S"
)
logging.getLogger("hazelcast").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def update_worker(client_id):
    client = hazelcast.HazelcastClient(
        cluster_name="my-hazelcast-cluster",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
    )
    my_map = client.get_map("race-map").blocking()
    key = "pessimistic-key"

    logger.info(f"[Client-{client_id}] Starting 10K increments with pessimistic lock")

    for _ in range(10000):
        my_map.lock(key)
        try:
            value = my_map.get(key)
            value["amount"] += 1
            my_map.put(key, value)
        finally:
            my_map.unlock(key)

    logger.info(f"[Client-{client_id}] Done")
    client.shutdown()


def main():
    logger.info("Initializing map and clients...")

    client = hazelcast.HazelcastClient(
        cluster_name="my-hazelcast-cluster",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
    )
    map = client.get_map("race-map").blocking()
    map.put("pessimistic-key", {"amount": 0})
    client.shutdown()

    start = time.time()

    processes = []
    for i in range(3):
        p = multiprocessing.Process(target=update_worker, args=(i + 1,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end = time.time()
    duration = end - start

    client = hazelcast.HazelcastClient(
        cluster_name="my-hazelcast-cluster",
        cluster_members=["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
    )
    result = client.get_map("race-map").blocking().get("pessimistic-key")["amount"]
    logger.info(f"Final value = {result} (expected: 30000)")
    logger.info(f"Total time taken: {duration:.2f} seconds")
    client.shutdown()


if __name__ == "__main__":
    main()
