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

print("Registering C1, C2, C3...")
s1, cid1, parts1 = register("payment", "order")
s2, cid2, parts2 = register("payment", "order")
s3, cid3, parts3 = register("payment", "order")

sb, cidb, partsb = register("group_b", "order")

print(f"C1 initially assigned: {parts1}")
print(f"C2 initially assigned: {parts2}")
print(f"C3 initially assigned: {parts3}")
print(f"Group B initially assigned: {partsb}")

print("\nClosing C3 and C1 to simulate failure (leaving only C2)...")
s3.close()
s1.close()

print("Waiting 16 seconds for heartbeat monitor to detect failure...")
for _ in range(16):
    heartbeat(s2, "payment", cid2)
    heartbeat(sb, "group_b", cidb)
    time.sleep(1)

print("\nPublishing to ensure we can consume from all partitions...")
for i in range(3):
    publish("order", {"data": f"msg_{i}"})

c2_consumed = set()
for _ in range(4):
    res = consume(s2, "payment", "order", cid2)
    if res and res["type"] != "EMPTY":
        c2_consumed.add(res.get("partition"))

print(f"C2 consumed from partitions: {c2_consumed} (Expected: {{0, 1, 2}})")

# Group B should still be independent
cb_consumed = set()
for _ in range(4):
    res = consume(sb, "group_b", "order", cidb)
    if res and res["type"] != "EMPTY":
        cb_consumed.add(res.get("partition"))

print(f"Group B consumed from partitions: {cb_consumed} (Expected: {{0, 1, 2}})")

s2.close()
sb.close()
