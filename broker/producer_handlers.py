from common.constants import (
    MESSAGE_TYPE_PUBLISH,
    MESSAGE_TYPE_CONSUME,
    MESSAGE_TYPE_ACK,
    MESSAGE_TYPE_REGISTER,
    MESSAGE_TYPE_HEARTBEAT
)
from common.protocol import recv_message
from broker.message_handlers import (
    handle_publish,
    handle_heartbeat,
    handle_consume,
    handle_ack,
    handle_register
)

from broker.storage import consumers, consumers_lock


def handle_client(client_socket, client_address):
    print(f"Client connected: {client_address}")
    consumer_id = None  # Initialize consumer_id to None
    try:
        while True:

            message = recv_message(client_socket)

            # Client disconnected
            if not message:
                print(f"Client disconnected: {client_address}")
                break

            if message["type"] == MESSAGE_TYPE_PUBLISH:
                handle_publish(message, client_socket)

            elif message["type"] == MESSAGE_TYPE_CONSUME:
                handle_consume(message, client_socket)

            elif message["type"] == MESSAGE_TYPE_ACK:
                handle_ack(message)
            elif message["type"] == MESSAGE_TYPE_REGISTER:
                consumer_id = handle_register(message, client_socket)
            elif message["type"] == MESSAGE_TYPE_HEARTBEAT:
                handle_heartbeat(message)

            else:
                print(f"Unknown message type: {message['type']}")

            print(message)
            print("Processed message")

    except Exception as e:
        print(f"Error while handling {client_address}: {e}")

    finally:
        if consumer_id is not None:

            with consumers_lock:

                if consumer_id in consumers:

                    consumers[consumer_id]["status"] = "DEAD"

                    print(
                        f"Consumer '{consumer_id}' "
                        f"disconnected. Marked as DEAD."
                    )
        client_socket.close()
        print(f"Connection closed: {client_address}")