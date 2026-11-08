import unittest
import struct
from common.protocol import decode_message
from broker.storage import (
    broker_id,
    consumers,
    topics,
    in_flight,
    pending_acks,
    consumer_offsets,
    retry_queue,
    next_offset,
    group_generations,
    partition_assignments
)
from broker.scheduler import next_partition_index
from broker.partition import Partition
from broker.message_handlers import handle_consume, handle_ack

class MockSocket:
    def __init__(self):
        self.sent_data = b""

    def sendall(self, data):
        self.sent_data += data

    def get_last_response(self):
        if not self.sent_data or len(self.sent_data) < 4:
            return None
        length = struct.unpack("!I", self.sent_data[:4])[0]
        data = self.sent_data[4:4 + length]
        return decode_message(data)

class TestConsumeAckOwnership(unittest.TestCase):
    def setUp(self):
        broker_id["value"] = 1  # Broker 1 owns partition 0
        consumers.clear()
        topics.clear()
        in_flight.clear()
        pending_acks.clear()
        consumer_offsets.clear()
        retry_queue.clear()
        next_offset.clear()
        next_partition_index.clear()
        group_generations.clear()
        partition_assignments.clear()

        # Topic "orders" with partition 0 on Broker 1
        topics["orders"] = {
            0: Partition(0)
        }
        topics["orders"][0].append({"message_id": 101, "topic": "orders", "payload": "hello"})

        # Register consumer C1 assigned to [0, 1]
        consumers["C1"] = {
            "group_id": "payment",
            "topic": "orders",
            "status": "ACTIVE",
            "partitions": [0, 1],
            "generation": 1
        }
        group_generations[("payment", "orders")] = 1
        partition_assignments["payment"] = {"orders": {0: "C1", 1: "C1"}}

    def test_consume_valid_owned_partition(self):
        sock = MockSocket()
        req = {
            "version": 1,
            "type": "CONSUME",
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 1
        }
        handle_consume(req, sock)
        resp = sock.get_last_response()
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("message_id"), 101)
        self.assertIn("payment", in_flight)
        self.assertIn(101, in_flight["payment"])

    def test_consume_rejected_on_non_owning_broker(self):
        sock = MockSocket()
        # Partition 1 is owned by Broker 2, but this broker is Broker 1
        req = {
            "version": 1,
            "type": "CONSUME",
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 1
        }
        handle_consume(req, sock)
        resp = sock.get_last_response()
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("type"), "ERROR")
        self.assertEqual(resp.get("payload", {}).get("code"), "NOT_PARTITION_OWNER")
        self.assertIn("Partition 1 belongs to Broker 2, not Broker 1", resp["payload"]["message"])

        # Ensure no state was mutated
        self.assertEqual(len(in_flight), 0)
        self.assertEqual(len(retry_queue), 0)

    def test_consume_without_explicit_partition_preserves_behavior(self):
        # When request does not explicitly pass "partition", it falls back to existing scheduler
        sock = MockSocket()
        req = {
            "version": 1,
            "type": "CONSUME",
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders"
        }
        handle_consume(req, sock)
        resp = sock.get_last_response()
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("message_id"), 101)

    def test_ack_valid_owned_partition(self):
        # Set up in_flight entry on Broker 1 for partition 0
        in_flight["payment"] = {
            101: {
                "message": {"topic": "orders", "partition": 0, "message_id": 101},
                "offset": 0,
                "consumer_id": "C1"
            }
        }
        ack_req = {
            "version": 1,
            "type": "ACK",
            "group_id": "payment",
            "consumer_id": "C1",
            "message_id": 101,
            "topic": "orders",
            "partition": 0
        }
        handle_ack(ack_req)
        # Message acknowledged and removed from in_flight
        self.assertNotIn(101, in_flight.get("payment", {}))
        # Offset 0 was advanced to 1 in consumer_offsets
        self.assertEqual(consumer_offsets.get("payment", {}).get("orders", {}).get(0), 1)

    def test_ack_rejected_on_non_owning_broker(self):
        # Set up in_flight entry with partition 1 (which belongs to Broker 2)
        in_flight["payment"] = {
            202: {
                "message": {"topic": "orders", "partition": 1, "message_id": 202},
                "offset": 0,
                "consumer_id": "C1"
            }
        }
        ack_req = {
            "version": 1,
            "type": "ACK",
            "group_id": "payment",
            "consumer_id": "C1",
            "message_id": 202,
            "topic": "orders",
            "partition": 1
        }
        handle_ack(ack_req)

        # Must NOT be deleted from in_flight
        self.assertIn(202, in_flight["payment"])
        # Must NOT be added to pending_acks or committed offsets
        self.assertNotIn("payment", pending_acks)
        self.assertNotIn("payment", consumer_offsets)

    def test_consume_rejected_when_partition_not_assigned(self):
        sock = MockSocket()
        # Set consumer C1 partitions to only [1] (partition 0 is not assigned to C1)
        consumers["C1"]["partitions"] = [1]
        partition_assignments["payment"]["orders"] = {1: "C1"}
        req = {
            "version": 1,
            "type": "CONSUME",
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 1
        }
        handle_consume(req, sock)
        resp = sock.get_last_response()
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("type"), "ERROR")
        self.assertEqual(resp.get("payload", {}).get("code"), "PARTITION_NOT_ASSIGNED")
        self.assertIn("Partition 0 is not assigned to consumer C1", resp["payload"]["message"])

    def test_malformed_ack_rejected_safely(self):
        # Non-dict or missing required fields
        handle_ack("not a dict")
        handle_ack({})
        handle_ack({"message_id": 123})  # missing group_id
        handle_ack({"group_id": "payment"})  # missing message_id
        self.assertEqual(len(in_flight), 0)
        self.assertEqual(len(pending_acks), 0)

    def test_multi_partition_offset_persistence_via_coordinator(self):
        import json
        from broker.offset_manager import commit_offset, OFFSET_FILE
        import broker.offset_manager as om
        from broker.message_handlers import handle_offset_commit

        # Setup coordinator hook so remote commit routes to handle_offset_commit
        def mock_coordinator_sync(group_id, topic, partition_id, offset):
            old_id = broker_id["value"]
            broker_id["value"] = 1
            try:
                handle_offset_commit({
                    "type": "OFFSET_COMMIT",
                    "group_id": group_id,
                    "topic": topic,
                    "partition": partition_id,
                    "offset": offset
                })
            finally:
                broker_id["value"] = old_id

        om.coordinator_offset_committer = mock_coordinator_sync
        try:
            # 1. Broker 1 commits partition 0
            broker_id["value"] = 1
            commit_offset("payment", "orders", 0, 19)

            with open(OFFSET_FILE, "r") as f:
                saved = json.load(f)
            self.assertEqual(saved["payment"]["orders"]["0"], 19)

            # 2. Broker 2 commits partition 1
            broker_id["value"] = 2
            commit_offset("payment", "orders", 1, 3)

            with open(OFFSET_FILE, "r") as f:
                saved = json.load(f)
            # Partition 0 must NOT be overwritten; partition 1 must be present!
            self.assertEqual(saved["payment"]["orders"]["0"], 19)
            self.assertEqual(saved["payment"]["orders"]["1"], 3)

            # 3. Broker 3 commits partition 2
            broker_id["value"] = 3
            commit_offset("payment", "orders", 2, 1)

            with open(OFFSET_FILE, "r") as f:
                saved = json.load(f)
            self.assertEqual(saved["payment"]["orders"], {
                "0": 19,
                "1": 3,
                "2": 1
            })
        finally:
            om.coordinator_offset_committer = None
            broker_id["value"] = 1

if __name__ == "__main__":
    unittest.main()

