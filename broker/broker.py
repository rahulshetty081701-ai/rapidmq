import socket
from broker.timeout_manager import monitor_timeouts
from common.constants import HOST, PORT
from broker.producer_handlers import handle_client
import threading
from broker.storage import topics
from broker.log_manager import load_messages

server_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

server_socket.bind((HOST,PORT))
topics.update(load_messages())
server_socket.listen()
print(f"RapidMQ Broker listening on {HOST}:{PORT}")

timeout_thread = threading.Thread(
    target=monitor_timeouts,
    daemon=True
)

timeout_thread.start()
print("Timeout monitor started...")

while True:
    client_socket, client_address = server_socket.accept()

    thread = threading.Thread(
        target=handle_client,
        args=(client_socket, client_address)
    )

    thread.start()