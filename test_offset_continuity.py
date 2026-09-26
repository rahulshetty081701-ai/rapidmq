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

def heartbeat(s, group_id, cid):
    send_msg(s, {"version": 1, "type": "HEARTBEAT", "group_id": group_id, "consumer_id": cid})

def consume(s, group_id, topic, cid):
    send_msg(s, {"version": 1, "type": "CONSUME", "group_id": group_id, "topic": topic, "consumer_id": cid})
    return recv_msg(s)

def ack(s, group_id, msg_id):
    send_msg(s, {"version": 1, "type": "ACK", "group_id": group_id, "message_id": msg_id})

print("Publishing 10 messages...")
for i in range(10):
    publish("order", {"data": f"msg_{i}"})
    
time.sleep(1)

print("\n--- Test 1/2: Reassignment and Multiple Partitions ---")
s1, cid1, parts1 = register("payment", "order")
s2, cid2, parts2 = register("payment", "order")
print(f"C1: {parts1}, C2: {parts2}")

res1 = consume(s1, "payment", "order", cid1)
res2 = consume(s1, "payment", "order", cid1)
if res1 and res1["type"] != "EMPTY":
    print(f"C1 consumed: {res1['payload']['data']} from P{res1['partition']} offset {res1['offset']}")
    ack(s1, "payment", res1["message_id"])
if res2 and res2["type"] != "EMPTY":
    print(f"C1 consumed: {res2['payload']['data']} from P{res2['partition']} offset {res2['offset']}")
    ack(s1, "payment", res2["message_id"])

print("Killing C1 and waiting 16s...")
s1.close()

for _ in range(16):
    heartbeat(s2, "payment", cid2)
    time.sleep(1)

print("C2 consuming after C1 failed...")
for _ in range(4):
    res = consume(s2, "payment", "order", cid2)
    if res and res["type"] != "EMPTY":
        print(f"C2 consumed: {res['payload']['data']} from P{res['partition']} offset {res['offset']}")

print("\n--- Test 3: Different groups ---")
s3, cid3, parts3 = register("group_b", "order")
res = consume(s3, "group_b", "order", cid3)
if res and res["type"] != "EMPTY":
    print(f"Group B consumed: {res['payload']['data']} from P{res['partition']} offset {res['offset']} (Expected offset 0)")

print("\n--- Test 4: Rebalance without failure ---")
s4, cid4, parts4 = register("payment", "order")
print(f"C4 joined. Consuming from C4...")
for _ in range(2):
    res = consume(s4, "payment", "order", cid4)
    if res and res["type"] != "EMPTY":
        print(f"C4 consumed: {res['payload']['data']} from P{res['partition']} offset {res['offset']}")

s2.close()
s3.close()
s4.close()
