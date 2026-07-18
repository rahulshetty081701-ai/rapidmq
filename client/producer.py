import socket
from common.constants import HOST, PORT, BUFFER_SIZE
import json

producer_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

producer_socket.connect((HOST,PORT))

print(f"conntected to rapidmq {HOST}:{PORT}")

message = {
    "version": 1,
    "type": "PUBLISH",
    "payload": {
        "message": "Hello RapidMQ"
    }
}

message = json.dumps(message)

message = message.encode("utf-8")

producer_socket.send(message)

message=producer_socket.recv(BUFFER_SIZE)

message = message.decode("utf-8")

message = json.loads(message)

print(message)

producer_socket.close()
