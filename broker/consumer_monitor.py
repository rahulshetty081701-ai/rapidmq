import time

from broker.storage import consumers, consumers_lock
from broker.message_handlers import release_consumer_partition

def consumer_heartbeat_monitor():
    """Monitor consumer heartbeats and release partitions for inactive consumers."""
    while True:
        time.sleep(10)

        current_time = time.time()

        dead_consumers = []
        with consumers_lock:
            for consumer_id, info in list(consumers.items()):
                if info.get("status") == "DEAD":
                    continue
                last_heartbeat = info.get("last_heartbeat", 0)
                if current_time - last_heartbeat > 15:  # 15 seconds timeout
                    print(
                        f"Consumer '{consumer_id}' is inactive. "
                        f"Last heartbeat was at {last_heartbeat}."
                    )
                    dead_consumers.append(consumer_id)

        for consumer_id in dead_consumers:
            release_consumer_partition(consumer_id)


