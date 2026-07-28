import json
import socket
from common.constants import HOST, PORT, BUFFER_SIZE , MESSAGE_TYPE_CONSUME
from common.protocol import encode_message , decode_message


consumer_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

consumer_socket.connect((HOST,PORT))

message = {
    "version": 1,
    "topic": "order",
    "type": MESSAGE_TYPE_CONSUME,
}


message = encode_message(message)

consumer_socket.send(message)

message = consumer_socket.recv(BUFFER_SIZE)

message = decode_message(message)

print(message)

consumer_socket.close()