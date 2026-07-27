from common.constants import HOST, PORT, BUFFER_SIZE,MESSAGE_TYPE_PUBLISH, MESSAGE_TYPE_CONSUME
from common.protocol import encode_message, decode_message
from broker.storage import messages , queue_lock
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
        with queue_lock:
            messages.append(message)

        encoded_ack = encode_message(ACK)

        client_socket.send(encoded_ack)

    elif message["type"] == MESSAGE_TYPE_CONSUME:
        with queue_lock:
            if len(messages) > 0:
                
                 response =   encode_message(messages.pop(0))
                
            else:
                response = {
                    "version": 1,
                    "type": "EMPTY",
                    "payload": {
                        "message": "Queue is empty"
                    }
                }

            client_socket.send(
                encode_message(response)
            )

    print(message)

    print("Sent to client")

    client_socket.close()