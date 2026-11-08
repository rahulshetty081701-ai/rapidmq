import socket
import argparse
from broker.consumer_monitor import consumer_heartbeat_monitor
from broker.timeout_manager import monitor_timeouts
from common.constants import HOST, PORT, KNOWN_BROKERS
from broker.producer_handlers import handle_client
import threading
from broker.storage import topics, dead_letter_queue, dead_letter_queue_lock
from broker.log_manager import load_messages
from broker.offset_manager import load_offsets
from broker.dlq_manager import load_dlq

parser = argparse.ArgumentParser(description="RapidMQ Broker")
parser.add_argument("--broker-id", type=int, default=1, help="Unique ID for this broker node")
parser.add_argument("--host", type=str, default=HOST, help="Host address to bind to")
parser.add_argument("--port", type=int, default=PORT, help="Port to bind to")
args = parser.parse_args()

BROKER_ID = args.broker_id
BROKER_HOST = args.host
BROKER_PORT = args.port

import broker.storage as storage
storage.broker_id["value"] = BROKER_ID

server_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

server_socket.bind((BROKER_HOST, BROKER_PORT))
topics.update(load_messages())
load_offsets()
loaded_dlq = load_dlq()

with dead_letter_queue_lock:
    dead_letter_queue.clear()
    dead_letter_queue.update(loaded_dlq)
server_socket.listen()
print(f"Broker {BROKER_ID} listening on {BROKER_HOST}:{BROKER_PORT}")
print("Known brokers:")
for b_id, (b_host, b_port) in KNOWN_BROKERS.items():
    print(f"  Broker {b_id} → {b_host}:{b_port}")

from broker.metadata import get_partition_owner
if topics:
    print("\nPartition Ownership Metadata:")
    for topic_name, partitions in topics.items():
        print(f"{topic_name}:")
        for partition_id in partitions.keys():
            owner = get_partition_owner(topic_name, partition_id)
            print(f"    P{partition_id} → Broker {owner}")
    print()

timeout_thread = threading.Thread(
    target=monitor_timeouts,
    daemon=True
)

timeout_thread.start()
print("Timeout monitor started...")

consumer_monitor_thread = threading.Thread(
    target=consumer_heartbeat_monitor,
    daemon=True
)

consumer_monitor_thread.start()
print("Consumer heartbeat monitor started...")

while True:
    client_socket, client_address = server_socket.accept()

    thread = threading.Thread(
        target=handle_client,
        args=(client_socket, client_address)
    )

    thread.start()