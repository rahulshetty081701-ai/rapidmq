from common.protocol import encode_message
from broker.storage import *
from broker.log_manager import append_message
import time , uuid
from broker.offset_manager import get_next_offset, advance_committed_offset, get_offset
from broker.partition import Partition
from broker.rebalance import rebalance
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

    if consumer.get("status") == "DEAD":
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": "Consumer is DEAD"
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

    # 1. Check retry queue first
    with retry_queue_lock:
        retry_entry = None
        partitions = consumer.get("partitions", [])
        for pid in partitions:
            if (
                group_id in retry_queue
                and topic in retry_queue[group_id]
                and pid in retry_queue[group_id][topic]
                and retry_queue[group_id][topic][pid]
            ):
                print(
                    f"Accessing retry queue for group '{group_id}', "
                    f"topic '{topic}', partition '{pid}'."
                )
                retry_entry = retry_queue[group_id][topic][pid].pop(0)
                break
                
        if retry_entry:
            response = retry_entry["message"]
            offset = retry_entry["offset"]
            retry_count = retry_entry.get("retry_count", 0) + 1
            
            # Extract the partition_id from the message itself
            partition_id = response.get("partition", 0)

            with in_flight_lock:
                if group_id not in in_flight:
                    in_flight[group_id] = {}

                in_flight[group_id][response["message_id"]] = {
                    "message": response,
                    "timestamp": time.time(),
                    "offset": offset,
                    "partition": partition_id,
                    "retry_count": retry_count,
                    "consumer_id": consumer_id
                }

            client_socket.sendall(encode_message(response))
            return
            
    # 2. Ask scheduler for a partition
    from broker.scheduler import select_partition
    partition_id = select_partition(consumer_id)
    
    if partition_id is None:
        response = {
            "version": 1,
            "type": "EMPTY",
            "payload": {
                "message": "No new messages available to consume"
            }
        }
        client_socket.sendall(encode_message(response))
        return

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
                            "message": "No new messages available to consume"
                        }
                    }

                else:
                    response = partition.messages[offset]
                    response["partition"] = partition_id

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
                            "retry_count": 0,
                            "consumer_id": consumer_id
                        }

                    next_offset[group_id][topic][partition_id] = offset + 1

    client_socket.sendall(encode_message(response))


def handle_ack(message):
    """Handle ACK messages: remove message from in-flight and process its offset."""

    print(f"Received ACK message")

    msg_id = message.get("message_id")
    group_id = message.get("group_id")

    consumer_id = message.get("consumer_id")

    with in_flight_lock:
        if msg_id not in in_flight.get(group_id, {}):
            print(f"Received ACK for unknown message ID: {msg_id}")
            return

        message_data = in_flight[group_id][msg_id]
        
        if consumer_id and message_data.get("consumer_id") and consumer_id != message_data["consumer_id"]:
            print(f"Stale ACK from {consumer_id} for msg {msg_id}")
            return

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

    # Store consumer information.
    with consumers_lock:
        consumers[consumer_id] = {
            "group_id": group_id,
            "topic": topic,
            "partitions": [],
            "last_heartbeat": time.time(),
            "status": "ACTIVE"
        }

    # Rebalance partitions for this group/topic
    rebalance(group_id, topic)

    with consumers_lock:
        assigned_partitions = consumers[consumer_id]["partitions"]

    response = {
        "version": 1,
        "type": "REGISTERED",
        "payload": {
            "consumer_id": consumer_id,
            "group_id": group_id,
            "topic": topic,
            "partitions": assigned_partitions
        }
    }

    client_socket.sendall(encode_message(response))

    print(
        f"Consumer '{consumer_id}' registered "
        f"to group '{group_id}', "
        f"topic '{topic}', "
        f"partitions '{assigned_partitions}'."
    )

    return consumer_id


def handle_heartbeat(message):
    consumer_id = message.get("consumer_id")

    with consumers_lock:
        if consumer_id not in consumers:
            print(f"Heartbeat from unknown consumer: {consumer_id}")
            return

        if consumers[consumer_id].get("status") == "DEAD":
            print(f"Heartbeat from DEAD consumer ignored: {consumer_id}")
            return

        consumers[consumer_id]["last_heartbeat"] = time.time()
        consumers[consumer_id]["status"] = "ACTIVE"

    print(f"Heartbeat received from consumer '{consumer_id}'")


def release_consumer_partition(consumer_id):
    """Release assigned partitions when a consumer dies."""
    group_id = None
    topic = None

    with consumers_lock:
        consumer = consumers.get(consumer_id)
        if not consumer:
            return
        consumer["status"] = "DEAD"
        group_id = consumer.get("group_id")
        topic = consumer.get("topic")

    if group_id is not None and topic is not None:
        to_release = []
        with in_flight_lock:
            if group_id in in_flight:
                for msg_id, data in list(in_flight[group_id].items()):
                    if data.get("consumer_id") == consumer_id:
                        to_release.append((msg_id, data))
                        del in_flight[group_id][msg_id]
                        
        if to_release:
            with retry_queue_lock:
                for msg_id, data in to_release:
                    t = data["message"]["topic"]
                    p = data["partition"]
                    if group_id not in retry_queue: retry_queue[group_id] = {}
                    if t not in retry_queue[group_id]: retry_queue[group_id][t] = {}
                    if p not in retry_queue[group_id][t]: retry_queue[group_id][t][p] = []
                    retry_queue[group_id][t][p].append(data)
            print(f"Re-queued {len(to_release)} in-flight messages for dead consumer '{consumer_id}'.")

        print(f"Consumer '{consumer_id}' died. Rebalancing group '{group_id}', topic '{topic}'.")
        from broker.rebalance import rebalance
        rebalance(group_id, topic)

