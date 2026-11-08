from broker.dlq_manager import append_to_dlq
from .storage import (
    in_flight,
    in_flight_lock,
    retry_queue,
    retry_queue_lock,
    dead_letter_queue,
    dead_letter_queue_lock,
    resolved_offsets,
    resolved_offsets_lock
)
import time

TIMEOUT = 30
MAX_RETRIES = 2


def requeue_or_dlq(group_id, message_id, data):
    """
    Re-queues an unacknowledged message to the retry queue, or routes it
    to the dead-letter queue if it has reached or exceeded MAX_RETRIES.
    Safely resolves offsets and advances committed offsets when DLQing.
    Guarantees no duplicate entries in retry queue.
    """
    topic = data["message"]["topic"]
    partition_id = data.get("partition", data.get("message", {}).get("partition", 0))

    if data.get("retry_count", 0) < MAX_RETRIES:
        with retry_queue_lock:
            if group_id not in retry_queue:
                retry_queue[group_id] = {}
            if topic not in retry_queue[group_id]:
                retry_queue[group_id][topic] = {}
            if partition_id not in retry_queue[group_id][topic]:
                retry_queue[group_id][topic][partition_id] = []

            # Prevent duplicate entries in retry queue
            if not any(
                entry.get("message", {}).get("message_id") == message_id
                for entry in retry_queue[group_id][topic][partition_id]
            ):
                retry_queue[group_id][topic][partition_id].append(data)

        print(
            f"Message {message_id} re-queued for retry "
            f"under group '{group_id}', topic '{topic}', partition '{partition_id}'."
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
                resolved_offsets[group_id][topic] = {}
            if partition_id not in resolved_offsets[group_id][topic]:
                resolved_offsets[group_id][topic][partition_id] = set()
            resolved_offsets[group_id][topic][partition_id].add(data["offset"])

        # Write to DLQ file
        append_to_dlq(group_id, topic, data)

        # Advance committed offset past the resolved dead-lettered message
        from broker.offset_manager import advance_committed_offset
        advance_committed_offset(group_id, topic, partition_id)

        print(
            f"Message {message_id} exceeded max retries "
            f"({MAX_RETRIES}) and was moved to DLQ."
        )


def check_timeouts(current=None):
    """Checks for timed-out in-flight messages and processes them."""
    if current is None:
        current = time.time()
    timed_out_messages = []

    # 1. Identify and remove timed-out messages from in-flight atomically
    with in_flight_lock:
        for group_id, messages in list(in_flight.items()):
            for message_id, data in list(messages.items()):
                if current - data["timestamp"] >= TIMEOUT:
                    timed_out_messages.append((group_id, message_id, data))
                    messages.pop(message_id, None)

    # 2. Process timed-out messages
    for group_id, message_id, data in timed_out_messages:
        requeue_or_dlq(group_id, message_id, data)


def monitor_timeouts():
    while True:
        time.sleep(1)
        check_timeouts()
