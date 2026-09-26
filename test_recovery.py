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

def ack(s, group_id, msg_id, cid):
    send_msg(s, {"version": 1, "type": "ACK", "group_id": group_id, "message_id": msg_id, "consumer_id": cid})

print("Publishing 10 messages...")
for i in range(10):
    publish("order", {"data": f"msg_{i}"})
    
time.sleep(1)

# TEST 1 & 4: Crash before ACK, multiple partitions
print("\n--- Test 1 & 4: Basic crash before ACK, multiple partitions ---")
s1, cid1, parts1 = register("payment", "order")
s2, cid2, parts2 = register("payment", "order")
print(f"C1 assigned: {parts1}, C2 assigned: {parts2}")

# C1 consumes 2 messages
c1_res1 = consume(s1, "payment", "order", cid1)
c1_res2 = consume(s1, "payment", "order", cid1)
print(f"C1 consumed: {c1_res1['payload']['data']} (P{c1_res1['partition']})")
print(f"C1 consumed: {c1_res2['payload']['data']} (P{c1_res2['partition']})")
print("C1 crashing...")
s1.close() # crash before ACK

# TEST 2: Crash after ACK
print("\n--- Test 2: Crash after ACK ---")
s3, cid3, parts3 = register("group_b", "order")
c3_res1 = consume(s3, "group_b", "order", cid3)
print(f"C3 consumed: {c3_res1['payload']['data']} (P{c3_res1['partition']})")
ack(s3, "group_b", c3_res1["message_id"], cid3)
print("C3 ACKs and crashes...")
s3.close()

# Wait for 16s so C1 and C3 die
print("Waiting 16 seconds for heartbeat monitor to detect deaths...")
for _ in range(16):
    heartbeat(s2, "payment", cid2)
    time.sleep(1)

print("\nC2 (survivor) consuming...")
# C2 should now get the messages C1 dropped
for _ in range(2):
    res = consume(s2, "payment", "order", cid2)
    if res and res["type"] != "EMPTY":
         print(f"C2 consumed (re-delivered): {res['payload']['data']} (P{res['partition']})")
         ack(s2, "payment", res["message_id"], cid2)

print("\n--- Test 5: Late ACK ---")
s4, cid4, parts4 = register("group_c", "order")
c4_res1 = consume(s4, "group_c", "order", cid4)
print(f"C4 consumed: {c4_res1['payload']['data']} (P{c4_res1['partition']})")
s4.close() # crash

print("Waiting 16 seconds for C4 death...")
time.sleep(16)
s5, cid5, parts5 = register("group_c", "order")
c5_res1 = consume(s5, "group_c", "order", cid5)
print(f"C5 consumed (re-delivered): {c5_res1['payload']['data']} (P{c5_res1['partition']})")

# C4 sends a late ACK for the message
s4_ghost = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s4_ghost.connect(('127.0.0.1', 9092))
print("Ghost C4 sending late ACK...")
ack(s4_ghost, "group_c", c4_res1["message_id"], cid4)
s4_ghost.close()

# C5 ACKs properly
print("C5 sending correct ACK...")
ack(s5, "group_c", c5_res1["message_id"], cid5)

print("\n--- Test 6: Timeout still works ---")
s6, cid6, parts6 = register("group_d", "order")
c6_res = consume(s6, "group_d", "order", cid6)
print(f"C6 consumed: {c6_res['payload']['data']}. Waiting 32 seconds for timeout...")
for _ in range(32):
    heartbeat(s6, "group_d", cid6)
    time.sleep(1)
c6_res_retry = consume(s6, "group_d", "order", cid6)
print(f"C6 consumed (timeout re-delivered): {c6_res_retry['payload']['data']}")
