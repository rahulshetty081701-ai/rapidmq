import socket
import threading
import time

from common.constants import (
    HOST,
    PORT,
    KNOWN_BROKERS,
    MESSAGE_TYPE_REGISTER,
    MESSAGE_TYPE_HEARTBEAT,
    MESSAGE_TYPE_ASSIGNMENT_UPDATE,
)

from common.protocol import encode_message, recv_message


class ConsumerClient:

    def __init__(self, group_id, topic, host=HOST, port=PORT, auto_connect=True):
        self.group_id = group_id
        self.topic = topic
        self.coordinator_host = host
        self.coordinator_port = port
        self.consumer_id = None
        self.assigned_partitions = []
        self.generation = 0
        self._current_partition_idx = 0
        self.heartbeat_interval = 5

        self.assignment_lock = threading.Lock()
        self.heartbeat_thread = None
        self.running = True

        # Data connections pool: broker_id -> socket
        self.data_sockets = {}
        self.sockets_lock = threading.Lock()

        # Dedicated coordinator socket for REGISTER and HEARTBEAT
        self.consumer_socket = None
        self.coordinator_socket_lock = threading.Lock()
        if auto_connect:
            self.connect()

    def connect(self):
        """Connect the dedicated coordinator socket."""
        with self.coordinator_socket_lock:
            if self.consumer_socket:
                try:
                    self.consumer_socket.close()
                except OSError:
                    pass
                self.consumer_socket = None

            try:
                self.consumer_socket = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_STREAM
                )
                self.consumer_socket.connect((self.coordinator_host, self.coordinator_port))
                return True
            except OSError as e:
                print(f"Failed to connect coordinator socket: {e}")
                self.consumer_socket = None
                return False

    def register(self):
        if not self.consumer_socket:
            self.connect()

        register_message = {
            "version": 1,
            "type": MESSAGE_TYPE_REGISTER,
            "group_id": self.group_id,
            "topic": self.topic
        }

        with self.coordinator_socket_lock:
            self.consumer_socket.sendall(
                encode_message(register_message)
            )

            response = recv_message(self.consumer_socket)
        if response is None or response.get("type") != "REGISTERED":
            raise Exception("Consumer registration failed.")

        payload = response.get("payload", {})
        self.consumer_id = payload.get("consumer_id")
        with self.assignment_lock:
            self.assigned_partitions = payload.get("partitions", [])
            self.generation = payload.get("generation", 1)

        print(
            f"Registered as consumer: {self.consumer_id}, "
            f"assigned partitions: {self.assigned_partitions}, "
            f"generation: {self.generation}"
        )

        self.start_heartbeat()

    def update_assignment(self, new_generation: int, new_partitions: list):
        """
        Updates assigned partitions and assignment generation thread-safely.
        Revokes partitions no longer assigned and resets index if needed.
        """
        with self.assignment_lock:
            if new_generation > self.generation or (new_generation == self.generation and set(self.assigned_partitions) != set(new_partitions)):
                old_partitions = set(self.assigned_partitions)
                self.generation = new_generation
                self.assigned_partitions = list(new_partitions)
                self._current_partition_idx = 0
                revoked = old_partitions - set(new_partitions)
                added = set(new_partitions) - old_partitions
                print(
                    f"Consumer {self.consumer_id} updated assignment: gen={self.generation}, "
                    f"partitions={self.assigned_partitions} (added={list(added)}, revoked={list(revoked)})"
                )

    def sync_assignment(self):
        """
        Queries the coordinator for the latest assignment and generation.
        Safe to call upon encountering STALE_GENERATION or PARTITION_NOT_ASSIGNED.
        """
        if not self.consumer_id:
            return None
        sync_message = {
            "version": 1,
            "type": MESSAGE_TYPE_ASSIGNMENT_UPDATE,
            "group_id": self.group_id,
            "consumer_id": self.consumer_id,
            "topic": self.topic
        }
        try:
            with self.coordinator_socket_lock:
                if not self.consumer_socket:
                    self.connect()
                if not self.consumer_socket:
                    return None
                self.consumer_socket.sendall(encode_message(sync_message))
                resp = recv_message(self.consumer_socket)
            if resp and resp.get("type") in ("ASSIGNMENT_UPDATE", "ASSIGNMENT_UPDATE_RESPONSE"):
                payload = resp.get("payload", {})
                gen = payload.get("generation")
                parts = payload.get("partitions", [])
                if gen is not None:
                    self.update_assignment(gen, parts)
                return resp
        except OSError as e:
            print(f"Failed to sync assignment from coordinator: {e}")
            with self.coordinator_socket_lock:
                if self.consumer_socket:
                    try:
                        self.consumer_socket.close()
                    except OSError:
                        pass
                    self.consumer_socket = None
        return None

    def start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True
        )
        self.heartbeat_thread.start()
        print(f"Heartbeat started for consumer '{self.consumer_id}'")

    def _heartbeat_loop(self):
        while self.running:
            with self.assignment_lock:
                current_gen = self.generation

            heartbeat_message = {
                "version": 1,
                "type": MESSAGE_TYPE_HEARTBEAT,
                "group_id": self.group_id,
                "consumer_id": self.consumer_id,
                "generation": current_gen
            }

            resp = None
            try:
                with self.coordinator_socket_lock:
                    if not self.consumer_socket and self.running:
                        self.connect()

                    if self.consumer_socket:
                        if hasattr(self.consumer_socket, "settimeout"):
                            old_timeout = self.consumer_socket.gettimeout()
                            self.consumer_socket.settimeout(2.0)
                            try:
                                self.consumer_socket.sendall(
                                    encode_message(heartbeat_message)
                                )
                                resp = recv_message(self.consumer_socket)
                            finally:
                                self.consumer_socket.settimeout(old_timeout)
                        else:
                            self.consumer_socket.sendall(
                                encode_message(heartbeat_message)
                            )
                            resp = recv_message(self.consumer_socket)

                if resp and resp.get("type") == "HEARTBEAT_RESPONSE":
                    payload = resp.get("payload", {})
                    resp_gen = payload.get("generation")
                    resp_parts = payload.get("partitions")
                    if resp_gen is not None and resp_parts is not None:
                        if resp_gen > current_gen or resp_parts != self.assigned_partitions:
                            self.update_assignment(resp_gen, resp_parts)
                print(f"Heartbeat sent by '{self.consumer_id}'")
            except (socket.timeout, TimeoutError):
                print(f"Heartbeat response timed out for '{self.consumer_id}'")
            except OSError as e:
                print(f"Heartbeat failed: {e}")
                with self.coordinator_socket_lock:
                    if self.consumer_socket:
                        try:
                            self.consumer_socket.close()
                        except OSError:
                            pass
                        self.consumer_socket = None

            time.sleep(self.heartbeat_interval)

    def resolve_partition_broker(self, partition: int):
        """
        Resolves the broker ID and (host, port) address for the given partition
        using the cluster metadata function and KNOWN_BROKERS.
        """
        from broker.metadata import get_partition_owner
        broker_id = get_partition_owner(self.topic, partition)
        broker_addr = KNOWN_BROKERS.get(broker_id)
        if not broker_addr:
            broker_addr = (self.coordinator_host, self.coordinator_port)
        return broker_id, broker_addr

    def _get_broker_socket(self, broker_id: int, host: str, port: int):
        """
        Retrieves or creates a dedicated data socket for the specified broker.
        Reuses existing connections to avoid opening new TCP connections per message.
        """
        with self.sockets_lock:
            sock = self.data_sockets.get(broker_id)
            if sock is not None:
                return sock

            try:
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.connect((host, port))
                self.data_sockets[broker_id] = new_sock
                print(f"Connected data connection to Broker {broker_id} at {host}:{port}")
                return new_sock
            except OSError as e:
                print(f"Failed to connect to Broker {broker_id} at {host}:{port}: {e}")
                return None

    def _close_broker_socket(self, broker_id: int):
        """Safely closes and removes a broker data socket on failure."""
        with self.sockets_lock:
            sock = self.data_sockets.pop(broker_id, None)
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass

    def consume(self, partition=None):
        """
        Fetches the next message from an assigned partition.
        Routes the CONSUME request to the broker that owns the partition.
        Reuses data connections across requests and handles connection failures gracefully.
        """
        with self.assignment_lock:
            assigned = list(self.assigned_partitions)
            current_gen = self.generation

        # If partition is explicitly specified, consume from it directly
        if partition is not None:
            if self.consumer_id and partition not in assigned:
                return {
                    "version": 1,
                    "type": "ERROR",
                    "payload": {
                        "code": "PARTITION_NOT_ASSIGNED",
                        "error": "PARTITION_NOT_ASSIGNED",
                        "message": f"Partition {partition} is not assigned to consumer {self.consumer_id}"
                    }
                }
            return self._consume_from_partition(partition, current_gen)

        # Fallback for single-broker / legacy mode where no partitions are assigned
        if not assigned:
            consume_message = {
                "version": 1,
                "type": "CONSUME",
                "topic": self.topic,
                "group_id": self.group_id,
                "consumer_id": self.consumer_id
            }
            if current_gen:
                consume_message["generation"] = current_gen
            target_socket = self.consumer_socket or self.data_sockets.get(1)
            if not target_socket:
                return {"version": 1, "type": "ERROR", "payload": {"message": "No connection available"}}
            try:
                target_socket.sendall(encode_message(consume_message))
                return recv_message(target_socket)
            except OSError as e:
                print(f"Failed to consume from coordinator: {e}")
                return None

        # Round-robin across assigned partitions
        num_partitions = len(assigned)
        last_empty_response = None

        for i in range(num_partitions):
            with self.assignment_lock:
                assigned = list(self.assigned_partitions)
                current_gen = self.generation
                if not assigned:
                    break
                num_partitions = len(assigned)
                idx = (self._current_partition_idx + i) % num_partitions
                target_partition = assigned[idx]

            resp = self._consume_from_partition(target_partition, current_gen)
            if resp is None:
                # Connection / communication failure on this broker, continue without crashing
                continue

            if resp.get("type") == "EMPTY":
                last_empty_response = resp
                continue

            if resp.get("type") == "ERROR":
                err_payload = resp.get("payload", {})
                err_code = (err_payload.get("code") or err_payload.get("error")) if isinstance(err_payload, dict) else None
                print(f"Error consuming partition {target_partition}: {err_payload}")
                if err_code in ("STALE_GENERATION", "PARTITION_NOT_ASSIGNED"):
                    self.sync_assignment()
                continue

            # Found a valid message! Advance index past this partition for fair scheduling
            with self.assignment_lock:
                self._current_partition_idx = (idx + 1) % num_partitions
            return resp

        # If all partitions were checked and none returned a message, advance index by 1
        with self.assignment_lock:
            if num_partitions > 0:
                self._current_partition_idx = (self._current_partition_idx + 1) % num_partitions
        if last_empty_response is not None:
            return last_empty_response

        return {
            "version": 1,
            "type": "EMPTY",
            "payload": {
                "message": "No new messages available to consume"
            }
        }

    def _consume_from_partition(self, partition: int, generation: int = None):
        broker_id, (b_host, b_port) = self.resolve_partition_broker(partition)
        sock = self._get_broker_socket(broker_id, b_host, b_port)
        if sock is None:
            return None

        consume_message = {
            "version": 1,
            "type": "CONSUME",
            "topic": self.topic,
            "group_id": self.group_id,
            "consumer_id": self.consumer_id,
            "partition": partition
        }
        if generation is not None:
            consume_message["generation"] = generation
        elif self.generation is not None:
            consume_message["generation"] = self.generation

        try:
            sock.sendall(encode_message(consume_message))
            response = recv_message(sock)
            if response is None:
                print(f"Broker {broker_id} disconnected during CONSUME.")
                self._close_broker_socket(broker_id)
                return None

            if isinstance(response, dict):
                if response.get("type") == "ERROR":
                    err_payload = response.get("payload", {})
                    err_code = (err_payload.get("code") or err_payload.get("error")) if isinstance(err_payload, dict) else None
                    if err_code in ("STALE_GENERATION", "PARTITION_NOT_ASSIGNED"):
                        self.sync_assignment()
                elif response.get("type") not in ("EMPTY", "ERROR"):
                    # Tag message with the delivering broker ID so ACK knows where to go
                    response["_delivered_by_broker_id"] = broker_id
                    if "topic" not in response:
                        response["topic"] = self.topic
                    if "partition" not in response:
                        response["partition"] = partition
                    if "generation" not in response and generation is not None:
                        response["generation"] = generation

            return response
        except OSError as e:
            print(f"Communication error with Broker {broker_id}: {e}")
            self._close_broker_socket(broker_id)
            return None

    def ack(self, message):
        """
        Sends an ACK for the delivered message to the broker that delivered it.
        The delivering broker holds the corresponding in-flight state.
        """
        if not isinstance(message, dict):
            print("Invalid message format for ACK: expected dict")
            return

        msg_id = message.get("message_id")
        if msg_id is None:
            print(f"Cannot ACK message without message_id: {message}")
            return

        topic = message.get("topic", self.topic)
        partition = message.get("partition")

        # 1. Identify the delivering broker
        broker_id = message.get("_delivered_by_broker_id")
        if broker_id is None and partition is not None:
            from broker.metadata import get_partition_owner
            broker_id = get_partition_owner(topic, partition)

        ack_message = {
            "version": 1,
            "type": "ACK",
            "group_id": self.group_id,
            "consumer_id": self.consumer_id,
            "message_id": msg_id,
            "topic": topic
        }
        if partition is not None:
            ack_message["partition"] = partition
        if "generation" in message and message["generation"] is not None:
            ack_message["generation"] = message["generation"]
        if "retry_count" in message and message["retry_count"] is not None:
            ack_message["retry_count"] = message["retry_count"]

        # 2. Route ACK to the delivering broker
        target_socket = None
        if broker_id is not None:
            from common.constants import KNOWN_BROKERS
            b_addr = KNOWN_BROKERS.get(broker_id)
            if b_addr:
                target_socket = self._get_broker_socket(broker_id, b_addr[0], b_addr[1])

        if target_socket is None:
            target_socket = self.consumer_socket

        if target_socket is None:
            print(f"Failed to send ACK for message {msg_id}: no socket connection")
            return

        try:
            target_socket.sendall(encode_message(ack_message))
            print(f"ACK sent for message {msg_id} (partition {partition}) to Broker {broker_id}")
        except OSError as e:
            print(f"Failed to send ACK for message {msg_id} to Broker {broker_id}: {e}")
            if broker_id is not None:
                self._close_broker_socket(broker_id)

    def close(self):
        self.running = False
        with self.coordinator_socket_lock:
            if self.consumer_socket:
                try:
                    self.consumer_socket.close()
                except OSError:
                    pass
                self.consumer_socket = None

        with self.sockets_lock:
            for b_id, sock in list(self.data_sockets.items()):
                try:
                    sock.close()
                except OSError:
                    pass
            self.data_sockets.clear()