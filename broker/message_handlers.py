import socket
import threading
from common.protocol import encode_message, recv_message
from common.constants import (
    KNOWN_BROKERS,
    COORDINATOR_BROKER_ID,
    HOST,
    PORT,
    MESSAGE_TYPE_VALIDATE_ASSIGNMENT,
    MESSAGE_TYPE_ASSIGNMENT_UPDATE,
    MESSAGE_TYPE_RELEASE_ASSIGNMENT,
    MESSAGE_TYPE_OFFSET_COMMIT
)
from broker.storage import *
from broker.log_manager import append_message
import time, uuid
from broker.offset_manager import get_next_offset, advance_committed_offset, get_offset
from broker.partition import Partition
from broker.rebalance import rebalance
from broker.timeout_manager import requeue_or_dlq

ACK = {
    "version": 1,
    "type": "ACK",
    "payload": {
        "status": "SUCCESS"
    }
}

coordinator_assignment_validator = None
_coordinator_conn_lock = threading.Lock()

def check_assignment_authoritative(group_id, topic, consumer_id, partition, req_generation):
    """
    Authoritatively checks assignment against coordinator state.
    Returns (is_valid, error_code, error_message).
    """
    with consumers_lock:
        consumer = consumers.get(consumer_id)
        if not consumer:
            return False, "CONSUMER_NOT_REGISTERED", f"Consumer '{consumer_id}' is not registered"
        if consumer.get("status") == "DEAD":
            return False, "CONSUMER_DEAD", f"Consumer '{consumer_id}' is DEAD"
        if consumer.get("group_id") != group_id or consumer.get("topic") != topic:
            return False, "INVALID_GROUP_TOPIC", f"Consumer '{consumer_id}' does not belong to group '{group_id}' / topic '{topic}'"

    with group_generations_lock:
        current_gen = group_generations.get((group_id, topic), 1)

    if req_generation != current_gen:
        return False, "STALE_GENERATION", f"Stale assignment generation: expected {current_gen}, got {req_generation}"

    with partition_assignments_lock:
        topic_assignments = partition_assignments.get(group_id, {}).get(topic, {})
        assigned_cid = topic_assignments.get(partition)
        if assigned_cid is None and isinstance(partition, str) and partition.isdigit():
            assigned_cid = topic_assignments.get(int(partition))
        elif assigned_cid is None and isinstance(partition, int):
            assigned_cid = topic_assignments.get(str(partition))
        if assigned_cid != consumer_id:
            return False, "PARTITION_NOT_ASSIGNED", f"Partition {partition} is not assigned to consumer {consumer_id}"

    return True, None, None

def query_coordinator_assignment(group_id, topic, consumer_id, partition, req_generation):
    """
    Queries coordinator broker over socket to validate assignment.
    """
    req = {
        "version": 1,
        "type": MESSAGE_TYPE_VALIDATE_ASSIGNMENT,
        "group_id": group_id,
        "topic": topic,
        "consumer_id": consumer_id,
        "partition": partition,
        "generation": req_generation
    }
    with _coordinator_conn_lock:
        coord_addr = KNOWN_BROKERS.get(COORDINATOR_BROKER_ID, (HOST, PORT))
        temp_sock = None
        resp = None
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.connect(coord_addr)
            temp_sock.sendall(encode_message(req))
            resp = recv_message(temp_sock)
        except OSError as e:
            print(f"Failed to query coordinator at {coord_addr}: {e}")
            return False, "COORDINATOR_UNAVAILABLE", f"Cannot contact coordinator: {e}"
        finally:
            if temp_sock:
                try:
                    temp_sock.close()
                except OSError:
                    pass

    if not resp or resp.get("type") != "VALIDATE_ASSIGNMENT_RESPONSE":
        return False, "COORDINATOR_ERROR", "Invalid response from coordinator"

    payload = resp.get("payload", {})
    if not payload.get("valid"):
        return False, payload.get("code", "VALIDATION_FAILED"), payload.get("message", "Assignment validation failed")

    return True, None, None

def validate_consumer_assignment(group_id, topic, consumer_id, partition, req_generation):
    if coordinator_assignment_validator is not None:
        return coordinator_assignment_validator(group_id, topic, consumer_id, partition, req_generation)

    BROKER_ID = broker_id["value"]
    if BROKER_ID == COORDINATOR_BROKER_ID:
        return check_assignment_authoritative(group_id, topic, consumer_id, partition, req_generation)
    else:
        return query_coordinator_assignment(group_id, topic, consumer_id, partition, req_generation)





def handle_publish(message, client_socket):
    topic = message.get("topic")
    
    if "partition" in message:
        partition_id = message["partition"]
    else:
        partition_id = next_partition.get(topic, 0)

    from broker.metadata import get_partition_owner
    BROKER_ID = broker_id["value"]
    expected_owner = get_partition_owner(topic, partition_id)

    if expected_owner != BROKER_ID:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "code": "WRONG_OWNER",
                "message": f"Partition {partition_id} belongs to Broker {expected_owner}, not Broker {BROKER_ID}"
            }
        }
        if client_socket:
            client_socket.sendall(encode_message(response))
        return

    with id_lock:
        message_id["value"] += 1
        message["message_id"] = message_id["value"]

    with queue_lock:
        if topic not in topics:
            topics[topic] = {
                0: Partition(0),
                1: Partition(1),
                2: Partition(2)
            }

        if "partition" not in message:
            next_partition[topic] = (partition_id + 1) % 3

        if partition_id not in topics[topic]:
            topics[topic][partition_id] = Partition(partition_id)

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

    if not topic or not group_id or not consumer_id:
        response = {
            "version": 1,
            "type": "ERROR",
            "payload": {
                "message": "Missing required fields (topic, group_id, consumer_id)"
            }
        }
        if client_socket:
            client_socket.sendall(encode_message(response))
        return

    # Enforce partition ownership and assignment fencing if partition is specified
    if "partition" in message:
        req_partition = message["partition"]
        from broker.metadata import get_partition_owner
        BROKER_ID = broker_id["value"]
        expected_owner = get_partition_owner(topic, req_partition)

        if expected_owner != BROKER_ID:
            response = {
                "version": 1,
                "type": "ERROR",
                "payload": {
                    "code": "NOT_PARTITION_OWNER",
                    "message": f"Partition {req_partition} belongs to Broker {expected_owner}, not Broker {BROKER_ID}"
                }
            }
            if client_socket:
                client_socket.sendall(encode_message(response))
            return

        if "generation" not in message:
            response = {
                "version": 1,
                "type": "ERROR",
                "payload": {
                    "code": "MISSING_GENERATION",
                    "message": "Assignment generation is required for partition consumption"
                }
            }
            if client_socket:
                client_socket.sendall(encode_message(response))
            return

        req_generation = message["generation"]
        is_valid, code, err_msg = validate_consumer_assignment(
            group_id, topic, consumer_id, req_partition, req_generation
        )
        if not is_valid:
            response = {
                "version": 1,
                "type": "ERROR",
                "payload": {
                    "code": code,
                    "message": err_msg
                }
            }
            if client_socket:
                client_socket.sendall(encode_message(response))
            return

        # Clean up any superseded in-flight messages for this partition
        to_requeue_superseded = []
        with in_flight_lock:
            if group_id in in_flight:
                for m_id, d in list(in_flight[group_id].items()):
                    d_topic = d.get("message", {}).get("topic", topic)
                    d_part = d.get("partition", d.get("message", {}).get("partition"))
                    if d_topic == topic and d_part == req_partition:
                        d_gen = d.get("generation")
                        d_cid = d.get("consumer_id")
                        if (d_gen is not None and d_gen < req_generation) or (d_cid and d_cid != consumer_id):
                            to_requeue_superseded.append((m_id, d))
                            del in_flight[group_id][m_id]

        for m_id, d in to_requeue_superseded:
            requeue_or_dlq(group_id, m_id, d)

        # Offset safety: ensure fetch pointer does not skip uncommitted messages on reassignment
        committed_offset = get_offset(group_id, topic, req_partition)
        with next_offset_lock:
            if group_id in next_offset and topic in next_offset[group_id] and req_partition in next_offset[group_id][topic]:
                if next_offset[group_id][topic][req_partition] > committed_offset:
                    has_retry = False
                    with retry_queue_lock:
                        has_retry = bool(retry_queue.get(group_id, {}).get(topic, {}).get(req_partition, []))
                    if not has_retry:
                        next_offset[group_id][topic][req_partition] = committed_offset

    else:
        # Legacy consumption without explicit partition (coordinator scheduler flow)
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
            if client_socket:
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
            if client_socket:
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
            if client_socket:
                client_socket.sendall(encode_message(response))
            return

        if "generation" in message:
            with group_generations_lock:
                current_gen = group_generations.get((group_id, topic), 1)
            if message["generation"] != current_gen:
                response = {
                    "version": 1,
                    "type": "ERROR",
                    "payload": {
                        "code": "STALE_GENERATION",
                        "message": f"Stale assignment generation: expected {current_gen}, got {message['generation']}"
                    }
                }
                if client_socket:
                    client_socket.sendall(encode_message(response))
                return

    # 1. Check retry queue first
    with retry_queue_lock:
        retry_entry = None
        partitions = [message["partition"]] if "partition" in message else (consumer.get("partitions", []) if consumer else [])
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
            response = dict(retry_entry["message"])
            offset = retry_entry["offset"]
            retry_count = retry_entry.get("retry_count", 0) + 1
            response["retry_count"] = retry_count
            
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
                    "generation": message.get("generation"),
                    "retry_count": retry_count,
                    "consumer_id": consumer_id
                }

            client_socket.sendall(encode_message(response))
            return
            
    # 2. Select target partition
    if "partition" in message:
        partition_id = message["partition"]
    else:
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
            from broker.metadata import get_topic_partitions
            if partition_id not in get_topic_partitions(topic):
                response = {
                    "version": 1,
                    "type": "ERROR",
                    "payload": {
                        "message": "Assigned partition does not exist"
                    }
                }
            else:
                response = {
                    "version": 1,
                    "type": "EMPTY",
                    "payload": {
                        "message": "No new messages available to consume"
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
                    response = dict(partition.messages[offset])
                    response["partition"] = partition_id
                    response["retry_count"] = 0

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
                            "generation": message.get("generation"),
                            "retry_count": 0,
                            "consumer_id": consumer_id
                        }

                    next_offset[group_id][topic][partition_id] = offset + 1

    client_socket.sendall(encode_message(response))


def handle_ack(message):
    """Handle ACK messages: remove message from in-flight and process its offset."""

    print(f"Received ACK message")

    if not isinstance(message, dict):
        print("Malformed ACK: message is not a dict")
        return

    msg_id = message.get("message_id")
    group_id = message.get("group_id")

    if msg_id is None or not group_id:
        print(f"Malformed ACK: missing message_id or group_id: {message}")
        return

    consumer_id = message.get("consumer_id")

    # If ACK explicitly carries topic and partition, validate ownership immediately
    from broker.metadata import get_partition_owner
    BROKER_ID = broker_id["value"]

    if "topic" in message and "partition" in message:
        ack_topic = message["topic"]
        ack_partition = message["partition"]
        expected_owner = get_partition_owner(ack_topic, ack_partition)
        if expected_owner != BROKER_ID:
            print(
                f"Rejected ACK for msg {msg_id}: Partition {ack_partition} of '{ack_topic}' "
                f"belongs to Broker {expected_owner}, not Broker {BROKER_ID}"
            )
            return

    with in_flight_lock:
        if msg_id not in in_flight.get(group_id, {}):
            print(f"Received ACK for unknown message ID: {msg_id}")
            return

        message_data = in_flight[group_id][msg_id]
        
        if consumer_id and message_data.get("consumer_id") and consumer_id != message_data["consumer_id"]:
            print(f"Stale ACK from {consumer_id} for msg {msg_id}")
            return

        if "generation" in message and message_data.get("generation") is not None:
            if message["generation"] != message_data["generation"]:
                print(f"Stale ACK generation from {consumer_id} for msg {msg_id}: expected {message_data['generation']}, got {message['generation']}")
                return

        if "retry_count" in message and message_data.get("retry_count") is not None:
            if message["retry_count"] < message_data["retry_count"]:
                print(f"Stale ACK from earlier retry attempt for msg {msg_id}: expected attempt {message_data['retry_count']}, got {message['retry_count']}")
                return

        offset = message_data["offset"]
        topic = message_data["message"]["topic"]
        partition_id = message_data["message"]["partition"]

        # Validate partition ownership from stored in-flight metadata before mutating state
        expected_owner = get_partition_owner(topic, partition_id)
        if expected_owner != BROKER_ID:
            print(
                f"Rejected ACK for msg {msg_id}: Partition {partition_id} "
                f"belongs to Broker {expected_owner}, not Broker {BROKER_ID}"
            )
            return

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

        partition_ids = list(topics[topic].keys()) #not using currently because we have rebalance now

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

    with group_generations_lock:
        current_gen = group_generations.get((group_id, topic), 1)

    response = {
        "version": 1,
        "type": "REGISTERED",
        "payload": {
            "consumer_id": consumer_id,
            "group_id": group_id,
            "topic": topic,
            "partitions": assigned_partitions,
            "generation": current_gen
        }
    }

    client_socket.sendall(encode_message(response))

    print(
        f"Consumer '{consumer_id}' registered "
        f"to group '{group_id}', "
        f"topic '{topic}', "
        f"partitions '{assigned_partitions}', "
        f"generation '{current_gen}'."
    )

    return consumer_id


def handle_heartbeat(message, client_socket=None):
    consumer_id = message.get("consumer_id")

    with consumers_lock:
        if consumer_id not in consumers:
            print(f"Heartbeat from unknown consumer: {consumer_id}")
            if client_socket and ("generation" in message or message.get("want_response", False)):
                resp = {
                    "version": 1,
                    "type": "ERROR",
                    "payload": {
                        "code": "CONSUMER_NOT_REGISTERED",
                        "message": f"Consumer '{consumer_id}' is not registered"
                    }
                }
                client_socket.sendall(encode_message(resp))
            return

        if consumers[consumer_id].get("status") == "DEAD":
            print(f"Heartbeat from DEAD consumer ignored: {consumer_id}")
            if client_socket and ("generation" in message or message.get("want_response", False)):
                resp = {
                    "version": 1,
                    "type": "ERROR",
                    "payload": {
                        "code": "CONSUMER_DEAD",
                        "message": f"Consumer '{consumer_id}' is DEAD"
                    }
                }
                client_socket.sendall(encode_message(resp))
            return

        consumers[consumer_id]["last_heartbeat"] = time.time()
        consumers[consumer_id]["status"] = "ACTIVE"
        group_id = consumers[consumer_id]["group_id"]
        topic = consumers[consumer_id]["topic"]
        partitions = list(consumers[consumer_id].get("partitions", []))

    with group_generations_lock:
        current_gen = group_generations.get((group_id, topic), 1)

    print(f"Heartbeat received from consumer '{consumer_id}'")

    if client_socket and ("generation" in message or message.get("want_response", False)):
        resp = {
            "version": 1,
            "type": "HEARTBEAT_RESPONSE",
            "payload": {
                "status": "SUCCESS",
                "consumer_id": consumer_id,
                "group_id": group_id,
                "topic": topic,
                "generation": current_gen,
                "partitions": partitions
            }
        }
        client_socket.sendall(encode_message(resp))


def notify_remote_release(owner_broker_id, group_id, topic, partition, consumer_id):
    """
    Sends a RELEASE_ASSIGNMENT message to a remote broker when a consumer dies or is revoked.
    """
    addr = KNOWN_BROKERS.get(owner_broker_id)
    if not addr:
        return
    msg = {
        "version": 1,
        "type": MESSAGE_TYPE_RELEASE_ASSIGNMENT,
        "group_id": group_id,
        "topic": topic,
        "partition": partition,
        "consumer_id": consumer_id
    }
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect(addr)
        s.sendall(encode_message(msg))
        recv_message(s)
        s.close()
    except OSError as e:
        print(f"Failed to notify remote Broker {owner_broker_id} of consumer release: {e}")


def handle_release_assignment(message, client_socket=None):
    """
    Handle RELEASE_ASSIGNMENT notification from coordinator or peer.
    Re-queues in-flight messages for the revoked consumer and protects next_offset.
    """
    group_id = message.get("group_id")
    topic = message.get("topic")
    partition = message.get("partition")
    consumer_id = message.get("consumer_id")

    to_requeue = []
    with in_flight_lock:
        if group_id in in_flight:
            for msg_id, data in list(in_flight[group_id].items()):
                d_topic = data.get("message", {}).get("topic", topic)
                d_part = data.get("partition", data.get("message", {}).get("partition"))
                d_cid = data.get("consumer_id")
                if d_topic == topic and (partition is None or d_part == partition):
                    if consumer_id is None or d_cid == consumer_id:
                        to_requeue.append((msg_id, data))
                        del in_flight[group_id][msg_id]

    for msg_id, data in to_requeue:
        requeue_or_dlq(group_id, msg_id, data)

    # Offset safety: ensure fetch pointer doesn't skip uncommitted messages
    if partition is not None:
        from broker.offset_manager import get_offset
        committed = get_offset(group_id, topic, partition)
        with next_offset_lock:
            if group_id in next_offset and topic in next_offset[group_id] and partition in next_offset[group_id][topic]:
                if next_offset[group_id][topic][partition] > committed:
                    has_retry = False
                    with retry_queue_lock:
                        has_retry = bool(retry_queue.get(group_id, {}).get(topic, {}).get(partition, []))
                    if not has_retry:
                        next_offset[group_id][topic][partition] = committed

    if client_socket:
        resp = {
            "version": 1,
            "type": "RELEASE_ASSIGNMENT_RESPONSE",
            "payload": {"status": "SUCCESS"}
        }
        client_socket.sendall(encode_message(resp))


def release_consumer_partition(consumer_id):
    """Release assigned partitions when a consumer dies."""
    group_id = None
    topic = None
    assigned_partitions = []

    with consumers_lock:
        consumer = consumers.get(consumer_id)
        if not consumer:
            return
        consumer["status"] = "DEAD"
        group_id = consumer.get("group_id")
        topic = consumer.get("topic")
        assigned_partitions = list(consumer.get("partitions", []))

    if group_id is not None and topic is not None:
        to_release = []
        with in_flight_lock:
            if group_id in in_flight:
                for msg_id, data in list(in_flight[group_id].items()):
                    if data.get("consumer_id") == consumer_id:
                        to_release.append((msg_id, data))
                        del in_flight[group_id][msg_id]
                        
        for msg_id, data in to_release:
            requeue_or_dlq(group_id, msg_id, data)

        # Notify remote partition owners to release in-flight messages for this consumer
        from broker.metadata import get_partition_owner
        BROKER_ID = broker_id["value"]
        for p_id in assigned_partitions:
            owner = get_partition_owner(topic, p_id)
            if owner != BROKER_ID:
                notify_remote_release(owner, group_id, topic, p_id, consumer_id)

        print(f"Consumer '{consumer_id}' died. Rebalancing group '{group_id}', topic '{topic}'.")
        from broker.rebalance import rebalance
        rebalance(group_id, topic)


def handle_metadata(message, client_socket):
    from common.constants import KNOWN_BROKERS
    from broker.metadata import get_partition_owner

    brokers_dict = {}
    for b_id, (b_host, b_port) in KNOWN_BROKERS.items():
        brokers_dict[str(b_id)] = {
            "host": b_host,
            "port": b_port
        }

    partition_owners = {}
    with queue_lock:
        for topic_name, partitions in topics.items():
            partition_owners[topic_name] = {}
            for partition_id in partitions.keys():
                owner = get_partition_owner(topic_name, partition_id)
                partition_owners[topic_name][str(partition_id)] = owner

    response = {
        "version": 1,
        "type": "METADATA_RESPONSE",
        "payload": {
            "brokers": brokers_dict,
            "partition_owners": partition_owners
        }
    }
    client_socket.sendall(encode_message(response))


def handle_validate_assignment(message, client_socket):
    """
    Handle VALIDATE_ASSIGNMENT requests sent by remote data brokers.
    Validates generation, partition ownership, and consumer status authoritatively.
    """
    group_id = message.get("group_id")
    topic = message.get("topic")
    consumer_id = message.get("consumer_id")
    partition = message.get("partition")
    req_generation = message.get("generation")

    is_valid, code, msg = check_assignment_authoritative(
        group_id, topic, consumer_id, partition, req_generation
    )
    with group_generations_lock:
        current_gen = group_generations.get((group_id, topic), 1)

    response = {
        "version": 1,
        "type": "VALIDATE_ASSIGNMENT_RESPONSE",
        "payload": {
            "valid": is_valid,
            "code": code,
            "message": msg,
            "current_generation": current_gen
        }
    }
    if client_socket:
        client_socket.sendall(encode_message(response))



def handle_assignment_update(message, client_socket):
    """
    Handle ASSIGNMENT_UPDATE requests from consumers desiring an immediate assignment refresh.
    """
    group_id = message.get("group_id")
    topic = message.get("topic")
    consumer_id = message.get("consumer_id")

    with consumers_lock:
        consumer = consumers.get(consumer_id)
        if not consumer or consumer.get("status") == "DEAD":
            response = {
                "version": 1,
                "type": "ERROR",
                "payload": {
                    "code": "CONSUMER_NOT_ACTIVE",
                    "message": f"Consumer '{consumer_id}' is not active"
                }
            }
            if client_socket:
                client_socket.sendall(encode_message(response))
            return
        partitions = list(consumer.get("partitions", []))

    with group_generations_lock:
        current_gen = group_generations.get((group_id, topic), 1)

    response = {
        "version": 1,
        "type": "ASSIGNMENT_UPDATE_RESPONSE",
        "payload": {
            "status": "SUCCESS",
            "consumer_id": consumer_id,
            "group_id": group_id,
            "topic": topic,
            "generation": current_gen,
            "partitions": partitions
        }
    }
    if client_socket:
        client_socket.sendall(encode_message(response))


def handle_offset_commit(message, client_socket=None):
    """
    Handle OFFSET_COMMIT requests sent by remote brokers to persist offsets on coordinator.
    """
    group_id = message.get("group_id")
    topic = message.get("topic")
    partition = message.get("partition")
    offset = message.get("offset")

    if group_id and topic and partition is not None and offset is not None:
        try:
            partition = int(partition)
        except (ValueError, TypeError):
            pass

        with consumer_offsets_lock:
            if group_id not in consumer_offsets:
                consumer_offsets[group_id] = {}
            if topic not in consumer_offsets[group_id]:
                consumer_offsets[group_id][topic] = {}
            consumer_offsets[group_id][topic][partition] = offset

        from broker.offset_manager import save_offsets
        save_offsets()
        print(f"Coordinator saved remote offset commit: group='{group_id}', topic='{topic}', partition='{partition}', offset={offset}")

    if client_socket:
        resp = {
            "version": 1,
            "type": "OFFSET_COMMIT_RESPONSE",
            "payload": {"status": "SUCCESS"}
        }
        try:
            client_socket.sendall(encode_message(resp))
        except OSError:
            pass

