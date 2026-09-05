from common.protocol import encode_message
from broker.storage import topics, queue_lock, message_id, id_lock, in_flight, in_flight_lock, consumer_offsets,pending_acks, pending_acks_lock,next_offset, next_offset_lock, retry_queue, retry_queue_lock,consumers, consumers_lock
from broker.log_manager import append_message
import time , uuid
from broker.offset_manager import get_next_offset,advance_committed_offset
from broker.partition import Partition
ACK = {
    "version": 1,
    "type": "ACK",
    "payload": {
        "status": "SUCCESS"
    }
}




def handle_publish(message, client_socket):
    """Handle publish messages: assign id, append to storage and topics, send ACK."""
    with id_lock:
        message_id["value"] += 1
        message["message_id"] = message_id["value"]
    topic = message.get("topic")
    append_message(topic, message)
    with queue_lock:
        if topic not in topics:
            topics[topic] = {
                0: Partition(0),
                1: Partition(1),
                2: Partition(2)
            }

        topics[topic][0].append(message)
    print(f"Published message to topic '{topic}': {message}")
    encoded_ack = encode_message(ACK)
    client_socket.sendall(encoded_ack)


def handle_consume(message, client_socket):
    """Handle consume messages: fetch next message and mark it in-flight."""
    topic = message.get("topic")
    group_id = message.get("group_id")
    with retry_queue_lock:
        if (
            group_id in retry_queue
            and topic in retry_queue[group_id]
            and retry_queue[group_id][topic]
        ):
            print(f"Accessing retry queue for group '{group_id}' and topic '{topic}'.")
            retry_entry = retry_queue[group_id][topic].pop(0)
            response = retry_entry["message"]
            offset = retry_entry["offset"]
            retry_count = retry_entry.get("retry_count", 0) + 1

            with in_flight_lock:
                if group_id not in in_flight:
                    in_flight[group_id] = {}

                in_flight[group_id][response["message_id"]] = {
                    "message": response,
                    "timestamp": time.time(),
                    "offset": offset,
                    "retry_count": retry_count
                }

        else:
            print(f"No messages in retry queue for group '{group_id}' and topic '{topic}'. Checking main queue.")
            with queue_lock:
                if topic not in topics or len(topics[topic]) == 0:
                    response = {
                        "version": 1,
                        "type": "EMPTY",
                        "payload": {
                            "message": "Queue is empty"
                        }
                    }

                else:
                    with next_offset_lock:
                        offset = get_next_offset(group_id, topic)

                        if offset >= len(topics[topic]):
                            response = {
                                "version": 1,
                                "type": "EMPTY",
                                "payload": {
                                    "message": "No new messages"
                                }
                            }

                        else:
                            response = topics[topic][offset]

                            with in_flight_lock:
                                if group_id not in in_flight:
                                    in_flight[group_id] = {}

                                in_flight[group_id][response["message_id"]] = {
                                    "message": response,
                                    "timestamp": time.time(),
                                    "offset": offset,
                                    "retry_count": 0
                                }

                            next_offset[group_id][topic] = offset + 1

    client_socket.sendall(encode_message(response))

def handle_ack(message):
    """Handle ACK messages: remove message from in-flight and process its offset."""
    msg_id = message.get("message_id")
    group_id = message.get("group_id")

    with in_flight_lock:
        if msg_id not in in_flight.get(group_id, {}):
            print(f"Received ACK for unknown message ID: {msg_id}")
            return

        message_data = in_flight[group_id][msg_id]
        offset = message_data["offset"]
        topic = message_data["message"]["topic"]

        del in_flight[group_id][msg_id]

    with pending_acks_lock:
        if group_id not in pending_acks:
            pending_acks[group_id] = {}

        if topic not in pending_acks[group_id]:
            pending_acks[group_id][topic] = set()

        pending_acks[group_id][topic].add(offset)

    advance_committed_offset(group_id, topic)

    print(f"Current in-flight messages after ACK: {list(in_flight.keys())}")
    print(f"Message {msg_id} acknowledged and removed from in-flight.")
    print(
        "Current consumer offsets for group '{}': {}".format(
            group_id,
            consumer_offsets.get(group_id, {})
        )
    )

def handle_register(message, client_socket):
    """Register a consumer and return its consumer ID."""

    group_id = message.get("group_id")

    if not group_id:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": "group_id is required"
            }
        }

        client_socket.sendall(encode_message(response))
        return

    consumer_id = str(uuid.uuid4())

    with consumers_lock:
        consumers[consumer_id] = {
            "group_id": group_id,
            "last_heartbeat": time.time(),
            "status": "ACTIVE"
        }

    response = {
        "version": 1,
        "type": "REGISTERED",
        "payload": {
            "consumer_id": consumer_id,
            "group_id": group_id
        }
    }

    client_socket.sendall(encode_message(response))

    print(
        f"Consumer '{consumer_id}' registered "
        f"to group '{group_id}'."
    )

    return consumer_id


def handle_heartbeat(message):
    consumer_id = message.get("consumer_id")

    with consumers_lock:
        if consumer_id not in consumers:
            print(f"Heartbeat from unknown consumer: {consumer_id}")
            return

        consumers[consumer_id]["last_heartbeat"] = time.time()
        consumers[consumer_id]["status"] = "ACTIVE"

    print(f"Heartbeat received from consumer '{consumer_id}'")