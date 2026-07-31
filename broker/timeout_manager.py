# TODO:
# Standardize lock ordering across the broker.
# Current monitor acquires:
#     in_flight_lock -> queue_lock
# while handle_consume() acquires:
#     queue_lock -> in_flight_lock
# Refactor after core broker features are complete.

from .storage import in_flight, topics, in_flight_lock, queue_lock
import time

TIMEOUT = 30

def monitor_timeouts():
    while True:

        time.sleep(1)

        current = time.time()

        messages_to_remove = []

        with in_flight_lock:

            for message_id, data in in_flight.items():

                if current - data["timestamp"] >= TIMEOUT:

                    topic = data["message"]["topic"]

                    print(
                        f"Message {message_id} timed out. Re-queuing to topic '{topic}'."
                    )

                    with queue_lock:
                        topics[topic].insert(0, data["message"])

                    messages_to_remove.append(message_id)

            for message_id in messages_to_remove:
                in_flight.pop(message_id, None)