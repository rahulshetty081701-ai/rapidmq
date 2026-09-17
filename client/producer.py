import socket
from common.constants import HOST, PORT, BUFFER_SIZE
import json
from common.protocol import encode_message, recv_message

producer_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

producer_socket.connect((HOST,PORT))

print(f"conntected to rapidmq {HOST}:{PORT}")

message = {
    "version": 1,
    "type": "PUBLISH",
    "topic":"order",
    "payload": {
        "message": "Check if message publishing is working after partition",
    }
}

message = encode_message(message)

producer_socket.sendall(message)

message=recv_message(producer_socket)

print(message)

producer_socket.close()

