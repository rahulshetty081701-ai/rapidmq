import unittest
import struct
import socket
from unittest.mock import patch

from common.protocol import encode_message, decode_message
from common.constants import HOST, PORT, KNOWN_BROKERS
from consumer.consumer_thread import ConsumerClient
from broker.metadata import get_partition_owner


class RecordingSocket:
    """Mock socket that records sent frames and replies from a queued list of responses."""

    def __init__(self, address=None):
        self.address = address
        self.sent_bytes = b""
        self.sent_messages = []
        self.queued_responses = []
        self.recv_buffer = b""
        self.closed = False
        self.raise_on_send = None
        self.raise_on_recv = None

    def sendall(self, data):
        if self.raise_on_send:
            raise self.raise_on_send
        self.sent_bytes += data
        # Decode framed message
        while len(data) >= 4:
            length = struct.unpack("!I", data[:4])[0]
            if len(data) < 4 + length:
                break
            msg_data = data[4:4 + length]
            self.sent_messages.append(decode_message(msg_data))
            data = data[4 + length:]

    def queue_response(self, response_dict):
        encoded = encode_message(response_dict)
        self.recv_buffer += encoded

    def recv(self, num_bytes):
        if self.raise_on_recv:
            raise self.raise_on_recv
        if not self.recv_buffer:
            return b""
        chunk = self.recv_buffer[:num_bytes]
        self.recv_buffer = self.recv_buffer[num_bytes:]
        return chunk

    def close(self):
        self.closed = True


class TestConsumerMultiBroker(unittest.TestCase):

    def setUp(self):
        self.coordinator_sock = RecordingSocket((HOST, PORT))
        self.broker1_sock = RecordingSocket(("127.0.0.1", 9092))
        self.broker2_sock = RecordingSocket(("127.0.0.1", 9093))
        self.broker3_sock = RecordingSocket(("127.0.0.1", 9094))

        self.b1_connections = [0]
        self.socket_map = {
            ("127.0.0.1", 9093): self.broker2_sock,
            ("127.0.0.1", 9094): self.broker3_sock,
        }

    def _socket_factory(self, family=socket.AF_INET, type=socket.SOCK_STREAM):
        socket_map = self.socket_map
        b1_connections = self.b1_connections

        class DelegatingSocket:
            def __init__(outer):
                outer.target = None
                outer.address = None

            def connect(outer, addr):
                outer.address = addr
                if addr == ("127.0.0.1", 9092):
                    if b1_connections[0] == 0:
                        b1_connections[0] += 1
                        outer.target = self.coordinator_sock
                    else:
                        outer.target = self.broker1_sock
                elif addr in socket_map:
                    outer.target = socket_map[addr]
                else:
                    raise ConnectionRefusedError(f"Connection refused to {addr}")

            def sendall(outer, data):
                if outer.target:
                    return outer.target.sendall(data)

            def recv(outer, num_bytes):
                if outer.target:
                    return outer.target.recv(num_bytes)
                return b""

            def close(outer):
                if outer.target:
                    outer.target.close()

        return DelegatingSocket()

    def test_partition_owner_resolution(self):
        """Verifies assigned partitions resolve to their correct broker owner."""
        client = ConsumerClient("payment", "orders", auto_connect=False)
        b1_id, b1_addr = client.resolve_partition_broker(0)
        b2_id, b2_addr = client.resolve_partition_broker(1)
        b3_id, b3_addr = client.resolve_partition_broker(2)

        self.assertEqual(b1_id, 1)
        self.assertEqual(b1_addr, ("127.0.0.1", 9092))
        self.assertEqual(b2_id, 2)
        self.assertEqual(b2_addr, ("127.0.0.1", 9093))
        self.assertEqual(b3_id, 3)
        self.assertEqual(b3_addr, ("127.0.0.1", 9094))

    def test_multi_broker_consume_and_ack_routing(self):
        """A consumer assigned partitions across Broker 1 and Broker 2 routes CONSUME and ACK to the correct broker."""
        with patch("socket.socket", side_effect=self._socket_factory):
            client = ConsumerClient("payment", "orders", auto_connect=True)

            # 1. Registration via coordinator socket
            self.coordinator_sock.queue_response({
                "version": 1,
                "type": "REGISTERED",
                "payload": {
                    "consumer_id": "C_TEST_1",
                    "group_id": "payment",
                    "topic": "orders",
                    "partitions": [0, 1]  # Partition 0 -> Broker 1, Partition 1 -> Broker 2
                }
            })
            client.register()
            self.assertEqual(client.consumer_id, "C_TEST_1")
            self.assertEqual(client.assigned_partitions, [0, 1])

            # Verify messages on coordinator were REGISTER and HEARTBEAT
            self.assertGreaterEqual(len(self.coordinator_sock.sent_messages), 1)
            self.assertEqual(self.coordinator_sock.sent_messages[0]["type"], "REGISTER")
            for m in self.coordinator_sock.sent_messages[1:]:
                self.assertEqual(m["type"], "HEARTBEAT")

            # 2. Queue messages on Broker 1 (partition 0) and Broker 2 (partition 1)
            self.broker1_sock.queue_response({
                "message_id": 101,
                "topic": "orders",
                "partition": 0,
                "payload": "Message on Broker 1"
            })
            self.broker2_sock.queue_response({
                "message_id": 201,
                "topic": "orders",
                "partition": 1,
                "payload": "Message on Broker 2"
            })

            # First consume -> Partition 0 -> Broker 1
            msg1 = client.consume()
            self.assertIsNotNone(msg1)
            self.assertEqual(msg1["message_id"], 101)
            self.assertEqual(msg1["partition"], 0)
            self.assertEqual(msg1["_delivered_by_broker_id"], 1)

            # Verify CONSUME request was sent to Broker 1, NOT Broker 2
            self.assertEqual(len(self.broker1_sock.sent_messages), 1)
            self.assertEqual(self.broker1_sock.sent_messages[0]["type"], "CONSUME")
            self.assertEqual(self.broker1_sock.sent_messages[0]["partition"], 0)
            self.assertEqual(len(self.broker2_sock.sent_messages), 0)

            # Send ACK for msg1 -> must go to Broker 1
            client.ack(msg1)
            self.assertEqual(len(self.broker1_sock.sent_messages), 2)
            self.assertEqual(self.broker1_sock.sent_messages[1]["type"], "ACK")
            self.assertEqual(self.broker1_sock.sent_messages[1]["message_id"], 101)
            self.assertEqual(self.broker1_sock.sent_messages[1]["partition"], 0)
            self.assertEqual(len(self.broker2_sock.sent_messages), 0)

            # Second consume -> Partition 1 -> Broker 2
            msg2 = client.consume()
            self.assertIsNotNone(msg2)
            self.assertEqual(msg2["message_id"], 201)
            self.assertEqual(msg2["partition"], 1)
            self.assertEqual(msg2["_delivered_by_broker_id"], 2)

            # Verify CONSUME request was sent to Broker 2
            self.assertEqual(len(self.broker2_sock.sent_messages), 1)
            self.assertEqual(self.broker2_sock.sent_messages[0]["type"], "CONSUME")
            self.assertEqual(self.broker2_sock.sent_messages[0]["partition"], 1)

            # Send ACK for msg2 -> must go to Broker 2
            client.ack(msg2)
            self.assertEqual(len(self.broker2_sock.sent_messages), 2)
            self.assertEqual(self.broker2_sock.sent_messages[1]["type"], "ACK")
            self.assertEqual(self.broker2_sock.sent_messages[1]["message_id"], 201)
            self.assertEqual(self.broker2_sock.sent_messages[1]["partition"], 1)

            client.close()

    def test_connection_reuse_across_messages(self):
        """Verifies that data sockets are pooled and reused across multiple messages."""
        with patch("socket.socket", side_effect=self._socket_factory):
            client = ConsumerClient("payment", "orders", auto_connect=True)
            client.consumer_id = "C_REUSE"
            client.assigned_partitions = [0, 1]

            # Queue 2 messages on Broker 1 and 2 on Broker 2
            for i in [1, 2]:
                self.broker1_sock.queue_response({
                    "message_id": 100 + i,
                    "topic": "orders",
                    "partition": 0,
                    "payload": f"B1 msg {i}"
                })
                self.broker2_sock.queue_response({
                    "message_id": 200 + i,
                    "topic": "orders",
                    "partition": 1,
                    "payload": f"B2 msg {i}"
                })

            # Consume all 4 messages
            for _ in range(4):
                msg = client.consume()
                self.assertIsNotNone(msg)
                client.ack(msg)

            # Check that client only created connections for broker 1 and broker 2
            self.assertIn(1, client.data_sockets)
            self.assertIn(2, client.data_sockets)
            self.assertEqual(len(client.data_sockets), 2)

            # Verify Broker 1 received 2 CONSUME + 2 ACK = 4 messages over the SAME socket
            self.assertEqual(len(self.broker1_sock.sent_messages), 4)
            # Verify Broker 2 received 2 CONSUME + 2 ACK = 4 messages over the SAME socket
            self.assertEqual(len(self.broker2_sock.sent_messages), 4)

            client.close()

    def test_single_broker_consumption_remaining_functional(self):
        """Single-broker operation routes all requests to Broker 1, and legacy fallback operates properly."""
        with patch("socket.socket", side_effect=self._socket_factory):
            client = ConsumerClient("payment", "orders", auto_connect=True)
            client.consumer_id = "C_SINGLE"
            # Single broker owns partitions 0 and 3 (if 3 partitions total, broker 1 owns 0)
            client.assigned_partitions = [0]

            self.broker1_sock.queue_response({
                "message_id": 10,
                "topic": "orders",
                "partition": 0,
                "payload": "single broker payload"
            })

            msg = client.consume()
            self.assertEqual(msg["message_id"], 10)
            self.assertEqual(len(self.broker1_sock.sent_messages), 1)

            client.ack(msg)
            self.assertEqual(len(self.broker1_sock.sent_messages), 2)
            self.assertEqual(self.broker1_sock.sent_messages[1]["type"], "ACK")

            # Legacy fallback: assigned_partitions is empty
            client.assigned_partitions = []
            self.coordinator_sock.queue_response({
                "message_id": 99,
                "topic": "orders",
                "payload": "fallback"
            })
            fallback_msg = client.consume()
            self.assertEqual(fallback_msg["message_id"], 99)
            # Sent without partition field
            last_sent = self.coordinator_sock.sent_messages[-1]
            self.assertEqual(last_sent["type"], "CONSUME")
            self.assertNotIn("partition", last_sent)

            client.close()

    def test_connection_failure_handled_without_crashing(self):
        """Connection failure to one broker is handled gracefully without crashing the consumer."""
        b1_conns = [0]

        def failing_socket_factory(family=socket.AF_INET, type=socket.SOCK_STREAM):
            class FailingDelegatingSocket:
                def __init__(outer):
                    outer.target = None
                    outer.address = None

                def connect(outer, addr):
                    outer.address = addr
                    if addr == ("127.0.0.1", 9093):
                        # Broker 2 is down!
                        raise ConnectionRefusedError("[Errno 111] Connection refused")
                    elif addr == ("127.0.0.1", 9092):
                        if b1_conns[0] == 0:
                            b1_conns[0] += 1
                            outer.target = self.coordinator_sock
                        else:
                            outer.target = self.broker1_sock
                    else:
                        raise ConnectionRefusedError(f"Connection refused to {addr}")

                def sendall(outer, data):
                    if outer.target:
                        return outer.target.sendall(data)

                def recv(outer, num_bytes):
                    if outer.target:
                        return outer.target.recv(num_bytes)
                    return b""

                def close(outer):
                    if outer.target:
                        outer.target.close()

            return FailingDelegatingSocket()

        with patch("socket.socket", side_effect=failing_socket_factory):
            client = ConsumerClient("payment", "orders", auto_connect=True)
            client.consumer_id = "C_FAIL"
            # Assigned to Partition 1 (Broker 2, down) and Partition 0 (Broker 1, up)
            client.assigned_partitions = [1, 0]

            self.broker1_sock.queue_response({
                "message_id": 105,
                "topic": "orders",
                "partition": 0,
                "payload": "survived failure"
            })

            # Consume: should attempt partition 1 (Broker 2 fails), skip to partition 0 (Broker 1 succeeds)
            msg = client.consume()
            self.assertIsNotNone(msg)
            self.assertEqual(msg["message_id"], 105)
            self.assertEqual(msg["partition"], 0)

            # Broker 2 socket failed to connect, so it is not in data_sockets
            self.assertNotIn(2, client.data_sockets)
            # Broker 1 succeeded
            self.assertIn(1, client.data_sockets)

            # Test sendall failure on ACK
            self.broker1_sock.raise_on_send = OSError("Broken pipe")
            # Should not crash
            client.ack(msg)
            # Failed socket was removed from data_sockets
            self.assertNotIn(1, client.data_sockets)

            client.close()

    def test_coordinator_connection_separate_from_data_connections(self):
        """Coordinator socket is maintained independently from data sockets."""
        with patch("socket.socket", side_effect=self._socket_factory):
            client = ConsumerClient("payment", "orders", auto_connect=True)
            client.consumer_id = "C_SEP"
            client.assigned_partitions = [0, 1]

            # Coordinator socket is self.consumer_socket
            self.assertIsNotNone(client.consumer_socket)
            self.assertEqual(len(client.data_sockets), 0)

            # Consuming opens data sockets
            self.broker1_sock.queue_response({"version": 1, "type": "EMPTY"})
            self.broker2_sock.queue_response({"version": 1, "type": "EMPTY"})
            client.consume()

            # Now data sockets exist for brokers 1 and 2
            self.assertIn(1, client.data_sockets)
            self.assertIn(2, client.data_sockets)
            self.assertNotEqual(client.consumer_socket, client.data_sockets[1])
            self.assertNotEqual(client.consumer_socket, client.data_sockets[2])

            # Closing client closes both coordinator and data sockets
            client.close()
            self.assertTrue(self.coordinator_sock.closed)
            self.assertTrue(self.broker1_sock.closed)
            self.assertTrue(self.broker2_sock.closed)
            self.assertEqual(len(client.data_sockets), 0)


if __name__ == "__main__":
    unittest.main()
