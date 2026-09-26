import socket
import json
import struct
import time

def encode_message(message):
    message_json = json.dumps(message).encode('utf-8')
    header = struct.pack('!I', len(message_json))
    return header + message_json

def decode_message(sock):
    header = sock.recv(4)
    if not header:
        return None
    message_length = struct.unpack('!I', header)[0]
    message_json = sock.recv(message_length).decode('utf-8')
    return json.loads(message_json)

def register_consumer(group_id, topic):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 9092))
    
    register_msg = {
        "version": 1,
        "type": "REGISTER",
        "group_id": group_id,
        "topic": topic
    }
    
    sock.sendall(encode_message(register_msg))
    response = decode_message(sock)
    
    print(f"Registered in {group_id}/{topic}: received partitions {response['payload']['partitions']}")
    return sock

print("--- Test 1: Register C1 ---")
c1_sock = register_consumer("payment", "order")
time.sleep(0.5)

print("\n--- Test 2: Register C2 ---")
c2_sock = register_consumer("payment", "order")
time.sleep(0.5)

print("\n--- Test 3: Register C3 ---")
c3_sock = register_consumer("payment", "order")
time.sleep(0.5)

print("\n--- Test 4: Register consumer in different group/topic ---")
other_sock = register_consumer("other_group", "order")

# Keep them alive for a moment so they don't get rebalanced away immediately 
# due to DEAD status if they close the socket right away.
# Actually, RapidMQ doesn't mark them DEAD just because socket closed unless 
# a heartbeat fails or read fails. But let's just close cleanly.
c1_sock.close()
c2_sock.close()
c3_sock.close()
other_sock.close()
