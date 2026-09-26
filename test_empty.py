import socket
import json
import struct

def send_msg(sock, msg):
    msg_json = json.dumps(msg).encode('utf-8')
    header = struct.pack('!I', len(msg_json))
    sock.sendall(header + msg_json)

def recv_msg(sock):
    header = sock.recv(4)
    if not header: return None
    length = struct.unpack('!I', header)[0]
    msg = sock.recv(length).decode('utf-8')
    return json.loads(msg)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 9092))
send_msg(s, {"version": 1, "type": "REGISTER", "group_id": "payment", "topic": "order"})
res = recv_msg(s)
print("REGISTERED:", res)

cid = res["payload"]["consumer_id"]
send_msg(s, {"version": 1, "type": "CONSUME", "group_id": "payment", "topic": "order", "consumer_id": cid})
res2 = recv_msg(s)
print("CONSUME:", res2)
