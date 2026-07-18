import socket
from common.constants import HOST, PORT, BUFFER_SIZE
import json

ACK={
    "version": 1,
    "type": "ACK",
    "payload": {
        "status": "SUCCESS"
    }
}

server_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

server_socket.bind((HOST,PORT))

server_socket.listen()
print(f"RapidMQ Broker listening on {HOST}:{PORT}")

client_socket, client_address = server_socket.accept()

print(f"Client connected: {client_address}")

message = client_socket.recv(BUFFER_SIZE)

message = message.decode("utf-8")

message = json.loads(message)

print(message)

ACK = json.dumps(ACK)

ACK = ACK.encode("utf-8")

client_socket.send(ACK)

print("sended to client")
client_socket.close()