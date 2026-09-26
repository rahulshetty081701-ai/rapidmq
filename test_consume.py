import socket
import json
import struct
import time

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

def publish(topic, payload):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))
    send_msg(s, {"version": 1, "type": "PUBLISH", "topic": topic, "payload": payload})
    res = recv_msg(s)
    s.close()
    return res

def register(group_id, topic):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', 9092))
    send_msg(s, {"version": 1, "type": "REGISTER", "group_id": group_id, "topic": topic})
    res = recv_msg(s)
    return s, res["payload"]["consumer_id"], res["payload"]["partitions"]

def consume(s, group_id, topic, cid):
    send_msg(s, {"version": 1, "type": "CONSUME", "group_id": group_id, "topic": topic, "consumer_id": cid})
    return recv_msg(s)

def ack(s, group_id, msg_id):
    send_msg(s, {"version": 1, "type": "ACK", "group_id": group_id, "message_id": msg_id})

print("Publishing 5 messages to 'order' to seed partitions...")
for i in range(5):
    publish("order", {"data": f"msg_{i}"})
    
time.sleep(1)

print("\n--- Test Registration & Consuming ---")
s1, cid1, parts1 = register("payment", "order")
s2, cid2, parts2 = register("payment", "order")

print(f"C1 assigned: {parts1}")
print(f"C2 assigned: {parts2}")

print("\nC1 Consuming...")
for _ in range(4):
    res = consume(s1, "payment", "order", cid1)
    if res["type"] == "EMPTY":
        print("C1: EMPTY")
    else:
        print(f"C1: msg_id={res['message_id']}, partition={res.get('partition')}")
        ack(s1, "payment", res["message_id"])
    time.sleep(0.1)

print("\nC2 Consuming...")
for _ in range(3):
    res = consume(s2, "payment", "order", cid2)
    if res["type"] == "EMPTY":
        print("C2: EMPTY")
    else:
        print(f"C2: msg_id={res['message_id']}, partition={res.get('partition')}")
        ack(s2, "payment", res["message_id"])
    time.sleep(0.1)

# All messages should be consumed now, let's verify empty behavior
print("\nVerifying empty behavior...")
res = consume(s1, "payment", "order", cid1)
print(f"C1: {res['type']}")

s1.close()
s2.close()
