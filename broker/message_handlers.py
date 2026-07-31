from common.protocol import encode_message
from broker.storage import topics, queue_lock, message_id, id_lock, in_flight, in_flight_lock
from broker.log_manager import append_message
import time

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
            topics[topic] = []
        topics[topic].append(message)
    print(f"Published message to topic '{topic}': {message}")
    encoded_ack = encode_message(ACK)
    client_socket.send(encoded_ack)


def handle_consume(message, client_socket):
    """Handle consume messages: pop from topic queue and mark in-flight."""
    topic = message.get("topic")
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
            response = topics[topic].pop(0)
            with in_flight_lock:
                in_flight[response["message_id"]] = {
                    "message": response,
                    "timestamp": time.time()
                }
            print(f"Consumed message from topic '{topic}': {response}")
            print(f"Current in-flight messages: {list(in_flight.keys())}")

    client_socket.send(encode_message(response))


def handle_ack(message):
    """Handle ACK messages: remove message from in-flight."""
    msg_id = message.get("message_id")
    with in_flight_lock:
        if msg_id in in_flight:
            del in_flight[msg_id]
            print(f"Current in-flight messages after ACK: {list(in_flight.keys())}")
            print(f"Message {msg_id} acknowledged and removed from in-flight.")
        else:
            print(f"Received ACK for unknown message ID: {msg_id}")
