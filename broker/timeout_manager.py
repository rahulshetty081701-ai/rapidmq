from broker.dlq_manager import append_to_dlq

from .storage import in_flight, in_flight_lock,retry_queue, retry_queue_lock,dead_letter_queue, dead_letter_queue_lock,resolved_offsets, resolved_offsets_lock
import time

TIMEOUT = 30
MAX_RETRIES = 2

def monitor_timeouts():
    while True:
        time.sleep(1)

        current = time.time()
        timed_out_messages = []

        # 1. Identify and remove timed-out messages from in-flight
        with in_flight_lock:
            for group_id, messages in in_flight.items():
                for message_id, data in list(messages.items()):
                    if current - data["timestamp"] >= TIMEOUT:
                        timed_out_messages.append(
                            (group_id, message_id, data)
                        )

                        messages.pop(message_id)

        # 2. Process timed-out messages without holding in-flight_lock
        for group_id, message_id, data in timed_out_messages:
            topic = data["message"]["topic"]

            if data["retry_count"] < MAX_RETRIES:

                with retry_queue_lock:
                    if group_id not in retry_queue:
                        retry_queue[group_id] = {}

                    if topic not in retry_queue[group_id]:
                        retry_queue[group_id][topic] = []

                    retry_queue[group_id][topic].append(data)

                print(
                    f"Message {message_id} re-queued for retry "
                    f"under group '{group_id}', topic '{topic}'."
                )

            else:
                with dead_letter_queue_lock:
                    if group_id not in dead_letter_queue:
                        dead_letter_queue[group_id] = {}

                    if topic not in dead_letter_queue[group_id]:
                        dead_letter_queue[group_id][topic] = []

                    dead_letter_queue[group_id][topic].append(data)

                with resolved_offsets_lock:
                    if group_id not in resolved_offsets:
                        resolved_offsets[group_id] = {}

                    if topic not in resolved_offsets[group_id]:
                        resolved_offsets[group_id][topic] = set()

                    resolved_offsets[group_id][topic].add(data["offset"])

                # No locks held during disk I/O
                append_to_dlq(group_id, topic, data)

                print(
                    f"Message {message_id} exceeded max retries "
                    f"({MAX_RETRIES}) and was moved to DLQ."
                )