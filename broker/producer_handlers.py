from common.constants import HOST, PORT, BUFFER_SIZE,MESSAGE_TYPE_PUBLISH, MESSAGE_TYPE_CONSUME
from common.protocol import encode_message, decode_message
from broker.storage import topics , queue_lock, message_id , id_lock
from broker.log_manager import append_message
import time
ACK={
    "version": 1,
    "type": "ACK",
    "payload": {
        "status": "SUCCESS"
    }
}

def handle_client(client_socket, client_address):
    print(f"Client connected: {client_address}")
    time.sleep(10)
    message = client_socket.recv(BUFFER_SIZE)

    message = decode_message(message)

    if message["type"] == MESSAGE_TYPE_PUBLISH:

        with id_lock:
            message_id["value"] += 1
            message["message_id"] = message_id["value"]
        topic = message["topic"]
        append_message(topic , message)
        with queue_lock:
            if topic not in topics:
                topics[topic] = []
            topics[topic].append(message)

        encoded_ack = encode_message(ACK)

        client_socket.send(encoded_ack)

    elif message["type"] == MESSAGE_TYPE_CONSUME:
        topic = message["topic"]
        with queue_lock:
            if topic not in topics or len(topics[topic]) == 0 :
                response = {
                                    "version": 1,
                                    "type": "EMPTY",
                                    "payload": {
                                        "message": "Queue is empty"
                                    }
                                }
                
            else:
                response = topics[topic].pop(0)

        client_socket.send(
            encode_message(response)
        )

    print(message)

    print("Sent to client")

    client_socket.close()