import socket, json, struct, time

def send_msg(sock, msg):
    msg_json = json.dumps(msg).encode('utf-8')
    sock.sendall(struct.pack('!I', len(msg_json)) + msg_json)

def recv_msg(sock):
    header = sock.recv(4)
    if not header: return None
    length = struct.unpack('!I', header)[0]
    return json.loads(sock.recv(length).decode('utf-8'))

# Publish a new message
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 9092))
send_msg(s, {"version": 1, "type": "PUBLISH", "topic": "order", "payload": {"data": "new_msg"}})
res = recv_msg(s)
s.close()

# Consume it
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 9092))
send_msg(s, {"version": 1, "type": "REGISTER", "group_id": "payment", "topic": "order"})
res = recv_msg(s)
cid = res["payload"]["consumer_id"]

send_msg(s, {"version": 1, "type": "CONSUME", "group_id": "payment", "topic": "order", "consumer_id": cid})
res = recv_msg(s)
print("CONSUMED:", res)
if res["type"] != "EMPTY":
    send_msg(s, {"version": 1, "type": "ACK", "group_id": "payment", "consumer_id": cid, "message_id": res["message_id"]})
    time.sleep(1) # wait for broker to process ACK and save

s.close()

# Check offsets.json
with open('storage/offsets.json', 'r') as f:
    print("OFFSETS.JSON:", f.read())
