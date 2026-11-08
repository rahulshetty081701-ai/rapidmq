import socket
import json
import threading
from common.constants import HOST, PORT, BUFFER_SIZE, MESSAGE_TYPE_METADATA
from common.protocol import encode_message, recv_message

class Producer:
    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.brokers = {}
        self.partition_owners = {}
        self.socket = None
        self.topic_partition_state = {}
        self.metadata_lock = threading.Lock()

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        print(f"Connected to RapidMQ at {self.host}:{self.port}")

    def fetch_metadata(self):
        with self.metadata_lock:
            req = {
                "version": 1,
                "type": MESSAGE_TYPE_METADATA
            }
            try:
                self.socket.sendall(encode_message(req))
                resp = recv_message(self.socket)
            except Exception as e:
                print(f"Error fetching metadata: {e}")
                return
                
            if resp and resp.get("type") == "METADATA_RESPONSE":
                payload = resp.get("payload")
                if not isinstance(payload, dict):
                    print("Invalid metadata response: payload is missing or not a dict")
                    return
                    
                brokers = payload.get("brokers")
                partition_owners = payload.get("partition_owners")
                
                if brokers is not None and partition_owners is not None:
                    self.brokers = brokers
                    self.partition_owners = partition_owners
                    print("Successfully fetched metadata")
                    print(f"Brokers: {self.brokers}")
                    print(f"Partition Owners: {self.partition_owners}")
                else:
                    print("Invalid metadata response: missing brokers or partition_owners")

    def resolve_broker(self, topic, partition):
        topic_meta = self.partition_owners.get(topic)
        if not topic_meta:
            print(f"Error: Topic '{topic}' metadata not found.")
            return None
        
        broker_id = topic_meta.get(str(partition))
        if broker_id is None:
            print(f"Error: Partition '{partition}' not found in topic '{topic}'.")
            return None
            
        broker_info = self.brokers.get(str(broker_id))
        if not broker_info:
            print(f"Error: Broker '{broker_id}' not found in known brokers.")
            return None
            
        return {
            "broker_id": int(broker_id),
            "host": broker_info.get("host"),
            "port": broker_info.get("port")
        }

    def publish(self, topic, payload):
        partitions = self.partition_owners.get(topic, {})
        partition_list = sorted([int(p) for p in partitions.keys()])
        
        if not partition_list:
            partition_list = [0, 1, 2]

        current_idx = self.topic_partition_state.get(topic, 0)
        partition = partition_list[current_idx % len(partition_list)]
        self.topic_partition_state[topic] = (current_idx + 1) % len(partition_list)

        message = {
            "version": 1,
            "type": "PUBLISH",
            "topic": topic,
            "partition": partition,
            "payload": payload
        }
        print(f"Client selected partition {partition} for topic '{topic}'")
        target_broker = self.resolve_broker(topic, partition)
        
        target_socket = self.socket
        temp_socket = None

        if target_broker:
            host = target_broker['host']
            port = target_broker['port']
            print(f"Resolved partition {partition} of '{topic}' to Broker {target_broker['broker_id']} ({host}:{port})")
            if host != self.host or port != self.port:
                try:
                    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    temp_socket.connect((host, port))
                    target_socket = temp_socket
                except Exception as e:
                    print(f"Error connecting to target broker: {e}")
                    # If we can't connect, let the request fail normally
                    pass

        try:
            target_socket.sendall(encode_message(message))
            resp = recv_message(target_socket)
        except Exception as e:
            print(f"Error publishing message: {e}")
            resp = None
            
        if temp_socket:
            temp_socket.close()

        if resp and resp.get("type") == "ERROR":
            payload = resp.get("payload", {})
            if payload.get("code") == "WRONG_OWNER":
                print(f"Ownership error detected: {payload.get('message', '')}. Refreshing metadata...")
                self.fetch_metadata()
            
        return resp

    def close(self):
        if self.socket:
            self.socket.close()

if __name__ == "__main__":
    producer = Producer()
    producer.connect()
    producer.fetch_metadata()
    
    print("\n--- Testing 'orders' topic ---")
    for i in range(4):
        resp = producer.publish("orders", {"message": f"Order {i}"})
    
    print("\n--- Testing 'payments' topic ---")
    for i in range(4):
        resp = producer.publish("payments", {"message": f"Payment {i}"})

    producer.close()

