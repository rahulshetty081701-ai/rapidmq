import socket
from common.constants import HOST, PORT
from broker.producer_handlers import handle_client
import threading

server_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

server_socket.bind((HOST,PORT))

server_socket.listen()
print(f"RapidMQ Broker listening on {HOST}:{PORT}")

while True:
    client_socket, client_address = server_socket.accept()

    thread = threading.Thread(
        target=handle_client,
        args=(client_socket, client_address)
    )

    thread.start()