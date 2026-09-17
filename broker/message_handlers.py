from common.protocol import encode_message
from broker.storage import *
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
    with id_lock:
        message_id["value"] += 1
        message["message_id"] = message_id["value"]

    topic = message.get("topic")

    with queue_lock:
        if topic not in topics:
            topics[topic] = {
                0: Partition(0),
                1: Partition(1),
                2: Partition(2)
            }

        partition_id = next_partition.get(topic, 0)

        next_partition[topic] = (partition_id + 1) % 3

        partition = topics[topic][partition_id]

        offset = partition.append(message)

        append_message(
            topic,
            partition_id,
            offset,
            message
        )

    print(
        f"Published message to topic '{topic}' "
        f"partition '{partition_id}': {message}"
    )

    if client_socket:
        encoded_ack = encode_message(ACK)
        client_socket.sendall(encoded_ack)

def handle_consume(message, client_socket):
    """Handle consume messages: fetch next message from assigned partition."""

    topic = message.get("topic")
    group_id = message.get("group_id")
    consumer_id = message.get("consumer_id")

    # Validate consumer
    with consumers_lock:
        consumer = consumers.get(consumer_id)

    if not consumer:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": "Consumer is not registered"
            }
        }
        client_socket.sendall(encode_message(response))
        return

    if consumer["group_id"] != group_id or consumer["topic"] != topic:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": "Consumer is not assigned to this group/topic"
            }
        }
        client_socket.sendall(encode_message(response))
        return

    partition_id = consumer["partition"]

    # Check retry queue first
    with retry_queue_lock:
        if (
            group_id in retry_queue
            and topic in retry_queue[group_id]
            and retry_queue[group_id][topic]
        ):
            print(
                f"Accessing retry queue for group '{group_id}', "
                f"topic '{topic}', partition '{partition_id}'."
            )

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
                    "partition": partition_id,
                    "retry_count": retry_count
                }

        else:
            print(
                f"No messages in retry queue for group '{group_id}' "
                f"and topic '{topic}'. Checking partition '{partition_id}'."
            )

            with queue_lock:
                if topic not in topics:
                    response = {
                        "version": 1,
                        "type": "EMPTY",
                        "payload": {
                            "message": "Topic does not exist"
                        }
                    }

                elif partition_id not in topics[topic]:
                    response = {
                        "version": 1,
                        "type": "ERROR",
                        "payload": {
                            "message": "Assigned partition does not exist"
                        }
                    }

                else:
                    print(
                        f"Fetching message for group '{group_id}', "
                        f"topic '{topic}', partition '{partition_id}'."
                    )

                    partition = topics[topic][partition_id]


                    with next_offset_lock:
                        offset = get_next_offset(
                            group_id,
                            topic,
                            partition_id
                        )

                        if offset >= len(partition.messages):
                            response = {
                                "version": 1,
                                "type": "EMPTY",
                                "payload": {
                                    "message": "No new messages123 available to consume"
                                }
                            }

                        else:
                            response = partition.messages[offset]

                            print(
                                f"Fetched message from group '{group_id}', "
                                f"topic '{topic}', partition '{partition_id}', "
                                f"offset {offset}: {response}"
                            )

                            with in_flight_lock:
                                if group_id not in in_flight:
                                    in_flight[group_id] = {}

                                in_flight[group_id][response["message_id"]] = {
                                    "message": response,
                                    "timestamp": time.time(),
                                    "offset": offset,
                                    "partition": partition_id,
                                    "retry_count": 0
                                }

                            next_offset[group_id][topic][partition_id] = offset + 1

    client_socket.sendall(encode_message(response))


def handle_ack(message):
    """Handle ACK messages: remove message from in-flight and process its offset."""

    print(f"Received ACK message")

    msg_id = message.get("message_id")
    group_id = message.get("group_id")

    with in_flight_lock:
        if msg_id not in in_flight.get(group_id, {}):
            print(f"Received ACK for unknown message ID: {msg_id}")
            return

        message_data = in_flight[group_id][msg_id]


        offset = message_data["offset"]
        topic = message_data["message"]["topic"]
        partition_id = message_data["message"]["partition"]

        del in_flight[group_id][msg_id]

    with pending_acks_lock:
        if group_id not in pending_acks:
            pending_acks[group_id] = {}

        if topic not in pending_acks[group_id]:
            pending_acks[group_id][topic] = {}

        if partition_id not in pending_acks[group_id][topic]:
            pending_acks[group_id][topic][partition_id] = set()

        pending_acks[group_id][topic][partition_id].add(offset)

    advance_committed_offset(
        group_id,
        topic,
        partition_id
    )

    print(f"Current in-flight messages after ACK: {list(in_flight.keys())}")

    print(
        f"Message {msg_id} acknowledged and removed from in-flight."
    )

    print(
        "Current consumer offsets for group '{}': {}".format(
            group_id,
            consumer_offsets.get(group_id, {})
        )
    )

def handle_register(message, client_socket):
    """Register a consumer and assign an available partition."""

    group_id = message.get("group_id")
    topic = message.get("topic")

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

    if not topic:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": "topic is required"
            }
        }

        client_socket.sendall(encode_message(response))
        return

    # Topic must already exist so that we know its partitions.
    with queue_lock:
        if topic not in topics:
            response = {
                "version": 1,
                "type": "ERROR",
                "payload": {
                    "message": f"Topic '{topic}' does not exist"
                }
            }

            client_socket.sendall(encode_message(response))
            return

        partition_ids = list(topics[topic].keys())

    consumer_id = str(uuid.uuid4())

    assigned_partition = None

    # Check + assign must happen atomically.
    with partition_assignments_lock:

        if group_id not in partition_assignments:
            partition_assignments[group_id] = {}

        if topic not in partition_assignments[group_id]:
            partition_assignments[group_id][topic] = {}

        assignments = partition_assignments[group_id][topic]

        # Find an idle partition.
        for partition_id in partition_ids:
            if partition_id not in assignments:
                assigned_partition = partition_id
                assignments[partition_id] = consumer_id
                break

    if assigned_partition is None:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": (
                    f"No available partition for topic '{topic}' "
                    f"in group '{group_id}'"
                )
            }
        }

        client_socket.sendall(encode_message(response))
        return

    # Store consumer information.
    with consumers_lock:
        consumers[consumer_id] = {
            "group_id": group_id,
            "topic": topic,
            "partition": assigned_partition,
            "last_heartbeat": time.time(),
            "status": "ACTIVE"
        }

    response = {
        "version": 1,
        "type": "REGISTERED",
        "payload": {
            "consumer_id": consumer_id,
            "group_id": group_id,
            "topic": topic,
            "partition": assigned_partition
        }
    }

    client_socket.sendall(encode_message(response))

    print(
        f"Consumer '{consumer_id}' registered "
        f"to group '{group_id}', "
        f"topic '{topic}', "
        f"partition '{assigned_partition}'."
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

