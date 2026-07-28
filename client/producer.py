import socket
from common.constants import HOST, PORT, BUFFER_SIZE
import json
from common.protocol import encode_message, decode_message

producer_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

producer_socket.connect((HOST,PORT))

print(f"conntected to rapidmq {HOST}:{PORT}")

message = {
    "version": 1,
    "type": "PUBLISH",
    "topic":"order",
    "payload": {
        "message": "Hello RapidMQ"
    }
}

message = encode_message(message)

producer_socket.send(message)

message=producer_socket.recv(BUFFER_SIZE)

message = decode_message(message)

print(message)

producer_socket.close()

