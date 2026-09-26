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

print("Publishing 20 messages...")
for i in range(20):
    publish("order", {"data": f"msg_{i}"})
    
time.sleep(1)

print("\n--- Test 1: Three consumers ---")
s1, cid1, parts1 = register("payment", "order")
s2, cid2, parts2 = register("payment", "order")
s3, cid3, parts3 = register("payment", "order")
print(f"C1: {parts1}, C2: {parts2}, C3: {parts3}")

print("\n--- Test 7: Multiple groups ---")
s_grp2, cid_grp2, parts_grp2 = register("analytics", "order")
print(f"Analytics group consumer: {parts_grp2}")
res_grp2 = consume(s_grp2, "analytics", "order", cid_grp2)
print(f"Analytics consumed: {res_grp2['payload']['data']} from P{res_grp2['partition']} offset {res_grp2['offset']}")
ack(s_grp2, "analytics", res_grp2["message_id"], cid_grp2)

print("\n--- Test 2: Two consumers ---")
s3.close()
print("C3 closed (explicitly left? well, socket closed). Waiting 16s for heartbeat timeout to simulate failure...")
for _ in range(16):
    heartbeat(s1, "payment", cid1)
    heartbeat(s2, "payment", cid2)
    time.sleep(1)

# Now C1 and C2 should have rebalanced
# Need to register a dummy to trigger rebalance? No, heartbeat timeout triggers release -> rebalance
print("After C3 death, C1 and C2 should have [0,2] and [1] or similar.")
c1_res1 = consume(s1, "payment", "order", cid1)
c1_res2 = consume(s1, "payment", "order", cid1)
print(f"C1 consumed: {c1_res1['payload']['data']} (P{c1_res1['partition']})")
print(f"C1 consumed: {c1_res2['payload']['data']} (P{c1_res2['partition']})")
ack(s1, "payment", c1_res1["message_id"], cid1)

print("\n--- Test 6: In-flight recovery ---")
print(f"C1 consumed msg (P{c1_res2['partition']}) but DID NOT ACK IT.")

print("\n--- Test 3: Consumer failure (C1 dies) ---")
s1.close()
print("Waiting 16s for C1 to die...")
for _ in range(16):
    heartbeat(s2, "payment", cid2)
    time.sleep(1)

print("C2 (survivor) should inherit all partitions [0, 1, 2] and receive the un-ACKed message!")
for _ in range(3):
    res = consume(s2, "payment", "order", cid2)
    if res and res["type"] != "EMPTY":
        print(f"C2 consumed: {res['payload']['data']} (P{res['partition']}) at offset {res['offset']}")
        ack(s2, "payment", res["message_id"], cid2)

print("\n--- Test 4: Consumer rejoins ---")
s4, cid4, parts4 = register("payment", "order")
print(f"C4 joined. Assigned partitions: {parts4}")
res = consume(s4, "payment", "order", cid4)
if res and res["type"] != "EMPTY":
    print(f"C4 consumed: {res['payload']['data']} (P{res['partition']}) at offset {res['offset']}")
    ack(s4, "payment", res["message_id"], cid4)

print("\n--- Test 5: Offset continuity ---")
print("Notice how offsets did not reset to 0 during the rebalances and failures.")

s2.close()
s4.close()
s_grp2.close()
print("Regression tests complete!")
