import socket
from broker.consumer_monitor import consumer_heartbeat_monitor
from broker.timeout_manager import monitor_timeouts
from common.constants import HOST, PORT
from broker.producer_handlers import handle_client
import threading
from broker.storage import topics, dead_letter_queue, dead_letter_queue_lock
from broker.log_manager import load_messages
from broker.offset_manager import load_offsets
from broker.dlq_manager import load_dlq

server_socket = socket.socket(socket.AF_INET , socket.SOCK_STREAM)

server_socket.bind((HOST,PORT))
topics.update(load_messages())
load_offsets()
loaded_dlq = load_dlq()

with dead_letter_queue_lock:
    dead_letter_queue.clear()
    dead_letter_queue.update(loaded_dlq)
server_socket.listen()
print(f"RapidMQ Broker listening on {HOST}:{PORT}")

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