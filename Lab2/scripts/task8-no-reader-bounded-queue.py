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

CLUSTER_CONFIG = {
    "cluster_name": "my-hazelcast-cluster",
    "cluster_members": ["127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703"]
}
QUEUE_NAME = "bounded-queue"


def producer(barrier):
    client = hazelcast.HazelcastClient(**CLUSTER_CONFIG)
    queue = client.get_queue(QUEUE_NAME).blocking()
    barrier.wait()
    logger.info("[Producer] started")

    for i in range(1, 101):
        while not queue.offer(f"{i}", timeout=1):
            pass
        logger.info(f"[Producer] inserted: {i}")

    logger.info("[Producer] finished")
    client.shutdown()


def main():
    logger.info("Starting Bounded Queue with 1 Producer, 0 Consumers")

    start = time.time()
    barrier = multiprocessing.Barrier(1)

    producer_proc = multiprocessing.Process(target=producer, args=(barrier,))

    producer_proc.start()
    producer_proc.join()

    logger.info(f"Total execution time: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    main()
