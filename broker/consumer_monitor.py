import time

from broker.storage import consumers, consumers_lock
def consumer_heartbeat_monitor():
    """Monitor consumer heartbeats and mark inactive consumers."""
    while True:
        time.sleep(10)

        current_time = time.time()

        with consumers_lock:
            for consumer_id, info in list(consumers.items()):
                last_heartbeat = info["last_heartbeat"]
                if current_time - last_heartbeat > 15:  # 15 seconds timeout
                    print(
                        f"Consumer '{consumer_id}' is inactive. "
                        f"Last heartbeat was at {last_heartbeat}."
                    )
                    info["status"] = "DEAD"


