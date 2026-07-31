from common.constants import (
    BUFFER_SIZE,
    MESSAGE_TYPE_PUBLISH,
    MESSAGE_TYPE_CONSUME,
    MESSAGE_TYPE_ACK,
)
from common.protocol import decode_message
from broker.message_handlers import (
    handle_publish,
    handle_consume,
    handle_ack,
)


def handle_client(client_socket, client_address):
    print(f"Client connected: {client_address}")

    try:
        while True:

            message = client_socket.recv(BUFFER_SIZE)

            # Client disconnected
            if not message:
                print(f"Client disconnected: {client_address}")
                break

            message = decode_message(message)

            if message["type"] == MESSAGE_TYPE_PUBLISH:
                handle_publish(message, client_socket)

            elif message["type"] == MESSAGE_TYPE_CONSUME:
                handle_consume(message, client_socket)

            elif message["type"] == MESSAGE_TYPE_ACK:
                handle_ack(message)

            else:
                print(f"Unknown message type: {message['type']}")

            print(message)
            print("Processed message")

    except Exception as e:
        print(f"Error while handling {client_address}: {e}")

    finally:
        client_socket.close()
        print(f"Connection closed: {client_address}")