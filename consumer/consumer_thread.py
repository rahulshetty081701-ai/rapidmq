import socket
import threading
import time

from common.constants import (
    HOST,
    PORT,
    MESSAGE_TYPE_REGISTER,
    MESSAGE_TYPE_HEARTBEAT,
)

from common.protocol import encode_message, recv_message


class ConsumerClient:

    def __init__(self, group_id, topic):
        self.group_id = group_id
        self.topic = topic
        self.consumer_id = None

        self.consumer_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        self.consumer_socket.connect((HOST, PORT))

        self.heartbeat_thread = None
        self.running = True

    def register(self):

        register_message = {
            "version": 1,
            "type": MESSAGE_TYPE_REGISTER,
            "group_id": self.group_id
        }

        self.consumer_socket.sendall(
            encode_message(register_message)
        )

        response = recv_message(self.consumer_socket)

        if response is None or response["type"] != "REGISTERED":
            raise Exception("Consumer registration failed.")

        self.consumer_id = response["payload"]["consumer_id"]

        print(
            f"Registered as consumer: {self.consumer_id}"
        )

        self.start_heartbeat()

    def start_heartbeat(self):

        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True
        )

        self.heartbeat_thread.start()

        print(
            f"Heartbeat started for consumer "
            f"'{self.consumer_id}'"
        )

    def _heartbeat_loop(self):

        while self.running:

            heartbeat_message = {
                "version": 1,
                "type": MESSAGE_TYPE_HEARTBEAT,
                "group_id": self.group_id,
                "consumer_id": self.consumer_id
            }

            try:

                self.consumer_socket.sendall(
                    encode_message(heartbeat_message)
                )

                print(
                    f"Heartbeat sent by "
                    f"'{self.consumer_id}'"
                )

            except OSError as e:

                print(f"Heartbeat failed: {e}")
                break

            time.sleep(5)

    def consume(self):

        consume_message = {
            "version": 1,
            "type": "CONSUME",
            "topic": self.topic,
            "group_id": self.group_id,
            "consumer_id": self.consumer_id
        }

        self.consumer_socket.sendall(
            encode_message(consume_message)
        )

        return recv_message(self.consumer_socket)

    def ack(self, message):

        ack_message = {
            "version": 1,
            "type": "ACK",
            "group_id": self.group_id,
            "consumer_id": self.consumer_id,
            "message_id": message["message_id"]
        }

        self.consumer_socket.sendall(
            encode_message(ack_message)
        )

        print(
            f"ACK sent for message "
            f"{message['message_id']}"
        )

    def close(self):

        self.running = False
        self.consumer_socket.close()