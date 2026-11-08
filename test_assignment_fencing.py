import unittest
import struct
import socket
import threading
import time
from common.protocol import encode_message, decode_message, recv_message
from common.constants import (
    HOST,
    PORT,
    KNOWN_BROKERS,
    MESSAGE_TYPE_REGISTER,
    MESSAGE_TYPE_CONSUME,
    MESSAGE_TYPE_ACK,
    MESSAGE_TYPE_HEARTBEAT,
    MESSAGE_TYPE_ASSIGNMENT_UPDATE,
    MESSAGE_TYPE_VALIDATE_ASSIGNMENT,
)
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
from broker.partition import Partition
from broker.scheduler import next_partition_index
from broker.rebalance import rebalance
from broker.message_handlers import (
    handle_consume,
    handle_ack,
    handle_register,
    handle_heartbeat,
    handle_validate_assignment,
    handle_assignment_update,
    release_consumer_partition,
    coordinator_assignment_validator,
)
from consumer.consumer_thread import ConsumerClient


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

    def get_all_responses(self):
        responses = []
        data = self.sent_data
        while len(data) >= 4:
            length = struct.unpack("!I", data[:4])[0]
            if len(data) < 4 + length:
                break
            msg_data = data[4:4 + length]
            responses.append(decode_message(msg_data))
            data = data[4 + length:]
        return responses


class TestAssignmentFencing(unittest.TestCase):
    def setUp(self):
        broker_id["value"] = 1  # Broker 1 is coordinator by default
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

        # Set up topic "orders" with partition 0, 1, 2
        topics["orders"] = {
            0: Partition(0),
            1: Partition(1),
            2: Partition(2)
        }

    def tearDown(self):
        import broker.message_handlers as mh
        mh.coordinator_assignment_validator = None

    def test_1_initial_assignment_and_generation_delivery(self):
        """1. Initial assignment and generation delivery."""
        sock = MockSocket()
        req = {
            "version": 1,
            "type": MESSAGE_TYPE_REGISTER,
            "group_id": "payment",
            "topic": "orders"
        }
        cid = handle_register(req, sock)
        resp = sock.get_last_response()

        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("type"), "REGISTERED")
        payload = resp.get("payload", {})
        self.assertEqual(payload.get("consumer_id"), cid)
        self.assertEqual(payload.get("group_id"), "payment")
        self.assertEqual(payload.get("topic"), "orders")
        self.assertEqual(payload.get("partitions"), [0, 1, 2])
        self.assertEqual(payload.get("generation"), 1)

        # Verify state in coordinator
        self.assertEqual(group_generations[("payment", "orders")], 1)
        self.assertEqual(consumers[cid]["generation"], 1)
        self.assertEqual(consumers[cid]["partitions"], [0, 1, 2])

    def test_2_generation_changes_after_rebalance(self):
        """2. Generation changes after a rebalance."""
        sock1 = MockSocket()
        cid1 = handle_register({"type": MESSAGE_TYPE_REGISTER, "group_id": "payment", "topic": "orders"}, sock1)
        self.assertEqual(group_generations[("payment", "orders")], 1)
        self.assertEqual(consumers[cid1]["partitions"], [0, 1, 2])

        # Second consumer joins -> assignment changes -> generation increments to 2
        sock2 = MockSocket()
        cid2 = handle_register({"type": MESSAGE_TYPE_REGISTER, "group_id": "payment", "topic": "orders"}, sock2)
        resp2 = sock2.get_last_response()
        self.assertEqual(resp2["payload"]["generation"], 2)
        self.assertEqual(group_generations[("payment", "orders")], 2)
        self.assertEqual(consumers[cid1]["generation"], 2)
        self.assertEqual(consumers[cid2]["generation"], 2)
        parts_1 = consumers[cid1]["partitions"]
        parts_2 = consumers[cid2]["partitions"]
        self.assertEqual(sorted(parts_1 + parts_2), [0, 1, 2])
        self.assertEqual(sorted([len(parts_1), len(parts_2)]), [1, 2])

        # Third consumer joins -> assignment changes -> generation increments to 3
        sock3 = MockSocket()
        cid3 = handle_register({"type": MESSAGE_TYPE_REGISTER, "group_id": "payment", "topic": "orders"}, sock3)
        self.assertEqual(group_generations[("payment", "orders")], 3)
        self.assertEqual(len(consumers[cid1]["partitions"]), 1)
        self.assertEqual(len(consumers[cid2]["partitions"]), 1)
        self.assertEqual(len(consumers[cid3]["partitions"]), 1)
        total_parts = consumers[cid1]["partitions"] + consumers[cid2]["partitions"] + consumers[cid3]["partitions"]
        self.assertEqual(sorted(total_parts), [0, 1, 2])

        # Rebalance called again without changes -> assignments don't change -> generation does not increment
        gen_before = group_generations[("payment", "orders")]
        rebalance("payment", "orders")
        self.assertEqual(group_generations[("payment", "orders")], gen_before)

        # Consumer disconnects/dies -> partitions redistributed -> generation increments
        release_consumer_partition(cid3)
        self.assertGreater(group_generations[("payment", "orders")], gen_before)
        new_gen = group_generations[("payment", "orders")]
        self.assertEqual(consumers[cid1]["generation"], new_gen)
        self.assertEqual(consumers[cid2]["generation"], new_gen)
        # Partitions redistributed between surviving consumers
        total_assigned = consumers[cid1]["partitions"] + consumers[cid2]["partitions"]
        self.assertEqual(sorted(total_assigned), [0, 1, 2])

    def test_3_consumer_receives_updated_assignments_and_stops_fetching_revoked(self):
        """3. Consumer receives updated assignments and stops fetching revoked partitions."""
        client = ConsumerClient("payment", "orders", auto_connect=False)
        client.consumer_id = "C_UPDATE_TEST"
        client.assigned_partitions = [0, 1]
        client.generation = 1

        # Simulate update via heartbeat or sync: partition 1 revoked, now only [0] at generation 2
        client.update_assignment(new_generation=2, new_partitions=[0])
        self.assertEqual(client.generation, 2)
        self.assertEqual(client.assigned_partitions, [0])

        # Record consumed partitions
        consumed_partitions = []

        def mock_consume_partition(part, gen):
            consumed_partitions.append((part, gen))
            return {"message_id": 999, "topic": "orders", "partition": part}

        client._consume_from_partition = mock_consume_partition

        # Consume 3 times
        for _ in range(3):
            client.consume()

        # All requests must be for partition 0, none for revoked partition 1
        self.assertEqual(len(consumed_partitions), 3)
        for part, gen in consumed_partitions:
            self.assertEqual(part, 0)
            self.assertEqual(gen, 2)

        # Explicit attempt to consume revoked partition 1 directly is rejected on client
        res = client.consume(partition=1)
        self.assertEqual(res["type"], "ERROR")
        self.assertEqual(res["payload"]["code"], "PARTITION_NOT_ASSIGNED")

    def test_4_current_assignments_accepted_by_appropriate_broker(self):
        """4. Current assignments are accepted by the appropriate broker."""
        # Setup authoritative state: Gen 2, C1 -> P0 (Broker 1), C2 -> P1 (Broker 2)
        group_generations[("payment", "orders")] = 2
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [0], "generation": 2}
        consumers["C2"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [1], "generation": 2}
        partition_assignments["payment"] = {"orders": {0: "C1", 1: "C2"}}

        topics["orders"][0].append({"message_id": 101, "topic": "orders", "payload": "for C1"})
        topics["orders"][1].append({"message_id": 201, "topic": "orders", "payload": "for C2"})

        # Broker 1 (coordinator) accepts C1 on partition 0
        broker_id["value"] = 1
        sock1 = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 2
        }, sock1)
        resp1 = sock1.get_last_response()
        self.assertEqual(resp1["message_id"], 101)

        # Broker 2 (remote broker) accepts C2 on partition 1 via coordinator validator hook
        broker_id["value"] = 2
        import broker.message_handlers as mh
        # Realistic double simulating coordinator check
        mh.coordinator_assignment_validator = mh.check_assignment_authoritative

        sock2 = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C2",
            "group_id": "payment",
            "topic": "orders",
            "partition": 1,
            "generation": 2
        }, sock2)
        resp2 = sock2.get_last_response()
        self.assertEqual(resp2["message_id"], 201)

    def test_5_stale_generations_rejected(self):
        """5. Stale generations are rejected by local and remote brokers."""
        group_generations[("payment", "orders")] = 2
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [0], "generation": 2}
        partition_assignments["payment"] = {"orders": {0: "C1"}}
        topics["orders"][0].append({"message_id": 101, "topic": "orders", "payload": "msg"})

        # Broker 1 local check with older generation 1
        broker_id["value"] = 1
        sock1 = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 1  # Stale!
        }, sock1)
        resp1 = sock1.get_last_response()
        self.assertEqual(resp1["type"], "ERROR")
        self.assertEqual(resp1["payload"]["code"], "STALE_GENERATION")

        # Future/invalid generation 99 is also rejected
        sock1_fut = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 99  # Invalid!
        }, sock1_fut)
        resp1_fut = sock1_fut.get_last_response()
        self.assertEqual(resp1_fut["type"], "ERROR")
        self.assertEqual(resp1_fut["payload"]["code"], "STALE_GENERATION")

        # Broker 2 remote check with stale generation 1
        broker_id["value"] = 2
        consumers["C2"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [1], "generation": 2}
        partition_assignments["payment"]["orders"][1] = "C2"
        topics["orders"][1].append({"message_id": 201, "topic": "orders", "payload": "msg"})

        import broker.message_handlers as mh
        mh.coordinator_assignment_validator = mh.check_assignment_authoritative

        sock2 = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C2",
            "group_id": "payment",
            "topic": "orders",
            "partition": 1,
            "generation": 1  # Stale!
        }, sock2)
        resp2 = sock2.get_last_response()
        self.assertEqual(resp2["type"], "ERROR")
        self.assertEqual(resp2["payload"]["code"], "STALE_GENERATION")

    def test_6_broker_rejects_unassigned_partition(self):
        """6. A broker rejects a partition that is not currently assigned to the consumer."""
        group_generations[("payment", "orders")] = 2
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [0], "generation": 2}
        consumers["C2"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [1], "generation": 2}
        partition_assignments["payment"] = {"orders": {0: "C1", 1: "C2"}}

        topics["orders"][0].append({"message_id": 101, "topic": "orders", "payload": "msg"})
        topics["orders"][1].append({"message_id": 201, "topic": "orders", "payload": "msg"})

        # C2 tries to consume Partition 0 on Broker 1 (not assigned to C2)
        broker_id["value"] = 1
        sock1 = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C2",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 2
        }, sock1)
        resp1 = sock1.get_last_response()
        self.assertEqual(resp1["type"], "ERROR")
        self.assertEqual(resp1["payload"]["code"], "PARTITION_NOT_ASSIGNED")
        self.assertIn("Partition 0 is not assigned to consumer C2", resp1["payload"]["message"])

        # C1 tries to consume Partition 1 on Broker 2 (not assigned to C1)
        broker_id["value"] = 2
        import broker.message_handlers as mh
        mh.coordinator_assignment_validator = mh.check_assignment_authoritative

        sock2 = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 1,
            "generation": 2
        }, sock2)
        resp2 = sock2.get_last_response()
        self.assertEqual(resp2["type"], "ERROR")
        self.assertEqual(resp2["payload"]["code"], "PARTITION_NOT_ASSIGNED")
        self.assertIn("Partition 1 is not assigned to consumer C1", resp2["payload"]["message"])

    def test_7_rejected_requests_do_not_mutate_state(self):
        """7. Rejected requests do not mutate offsets, retry queues, or in-flight state."""
        broker_id["value"] = 1
        group_generations[("payment", "orders")] = 2
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": [0], "generation": 2}
        partition_assignments["payment"] = {"orders": {0: "C1"}}

        # Partition 0 has messages at offset 0 and 1
        topics["orders"][0].append({"message_id": 101, "topic": "orders", "payload": "msg0", "partition": 0})
        topics["orders"][0].append({"message_id": 102, "topic": "orders", "payload": "msg1", "partition": 0})

        # Retry queue has an entry for partition 0
        retry_queue["payment"] = {
            "orders": {
                0: [{
                    "message": {"message_id": 999, "topic": "orders", "payload": "retry", "partition": 0},
                    "offset": 99,
                    "retry_count": 1,
                    "partition": 0
                }]
            }
        }
        next_offset["payment"] = {"orders": {0: 0}}

        initial_retry_count = len(retry_queue["payment"]["orders"][0])
        initial_next_offset = next_offset["payment"]["orders"][0]
        self.assertEqual(len(in_flight), 0)

        # 1. Send stale generation request
        sock_stale = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "C1",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 1  # Stale
        }, sock_stale)
        self.assertEqual(sock_stale.get_last_response()["type"], "ERROR")

        # 2. Send unassigned consumer request
        sock_unassigned = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": "UNKNOWN",
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": 2
        }, sock_unassigned)
        self.assertEqual(sock_unassigned.get_last_response()["type"], "ERROR")

        # Verify nothing mutated
        self.assertEqual(len(retry_queue["payment"]["orders"][0]), initial_retry_count)
        self.assertEqual(retry_queue["payment"]["orders"][0][0]["message"]["message_id"], 999)
        self.assertEqual(next_offset["payment"]["orders"][0], initial_next_offset)
        self.assertEqual(len(in_flight), 0)

    def test_8_existing_single_broker_behavior(self):
        """8. Existing single-broker behavior remains functional."""
        broker_id["value"] = 1
        sock = MockSocket()

        # Registration
        cid = handle_register({"type": MESSAGE_TYPE_REGISTER, "group_id": "payment", "topic": "orders"}, sock)
        reg_resp = sock.get_last_response()
        self.assertEqual(reg_resp["type"], "REGISTERED")
        gen = reg_resp["payload"]["generation"]

        # Append message
        topics["orders"][0].append({"message_id": 55, "topic": "orders", "payload": "hello single broker", "partition": 0})

        # Consume
        sock_consume = MockSocket()
        handle_consume({
            "version": 1,
            "type": MESSAGE_TYPE_CONSUME,
            "consumer_id": cid,
            "group_id": "payment",
            "topic": "orders",
            "partition": 0,
            "generation": gen
        }, sock_consume)
        c_resp = sock_consume.get_last_response()
        self.assertEqual(c_resp["message_id"], 55)

        # In-flight is populated
        self.assertIn("payment", in_flight)
        self.assertIn(55, in_flight["payment"])

        # ACK
        handle_ack({
            "version": 1,
            "type": MESSAGE_TYPE_ACK,
            "group_id": "payment",
            "consumer_id": cid,
            "message_id": 55,
            "topic": "orders",
            "partition": 0,
            "generation": gen
        })

        # Removed from in-flight and offset advanced
        self.assertNotIn(55, in_flight.get("payment", {}))
        self.assertEqual(consumer_offsets.get("payment", {}).get("orders", {}).get(0), 1)

    def test_9_real_socket_inter_broker_fencing(self):
        """Verifies remote-broker fencing over real TCP socket with coordinator server thread."""
        # Start coordinator TCP server on dynamic loopback port
        coord_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coord_server.bind(("127.0.0.1", 0))
        coord_port = coord_server.getsockname()[1]
        coord_server.listen(5)

        coord_running = [True]

        def coordinator_listener():
            while coord_running[0]:
                try:
                    coord_server.settimeout(0.5)
                    client_s, _ = coord_server.accept()
                except (socket.timeout, OSError):
                    continue
                try:
                    msg = recv_message(client_s)
                    if msg and msg.get("type") == MESSAGE_TYPE_VALIDATE_ASSIGNMENT:
                        handle_validate_assignment(msg, client_s)
                    client_s.close()
                except OSError:
                    pass

        t = threading.Thread(target=coordinator_listener, daemon=True)
        t.start()

        try:
            # Set up coordinator authoritative state
            group_generations[("payment", "orders")] = 2
            consumers["C_REMOTE"] = {
                "group_id": "payment",
                "topic": "orders",
                "status": "ACTIVE",
                "partitions": [1],
                "generation": 2
            }
            partition_assignments["payment"] = {"orders": {1: "C_REMOTE"}}
            topics["orders"][1].append({"message_id": 303, "topic": "orders", "payload": "tcp msg", "partition": 1})

            # Point KNOWN_BROKERS coordinator (id 1) to dynamic port
            orig_known = dict(KNOWN_BROKERS)
            KNOWN_BROKERS[1] = ("127.0.0.1", coord_port)

            # Broker 2 acts as data broker (not coordinator)
            broker_id["value"] = 2
            import broker.message_handlers as mh
            mh.coordinator_assignment_validator = None  # Use real query_coordinator_assignment over TCP!

            # 1. Valid request to Broker 2 -> accepts after querying Coordinator over real TCP
            sock_valid = MockSocket()
            handle_consume({
                "version": 1,
                "type": MESSAGE_TYPE_CONSUME,
                "consumer_id": "C_REMOTE",
                "group_id": "payment",
                "topic": "orders",
                "partition": 1,
                "generation": 2
            }, sock_valid)
            resp_valid = sock_valid.get_last_response()
            self.assertEqual(resp_valid["message_id"], 303)

            # 2. Stale request to Broker 2 -> rejected after querying Coordinator over real TCP
            sock_stale = MockSocket()
            handle_consume({
                "version": 1,
                "type": MESSAGE_TYPE_CONSUME,
                "consumer_id": "C_REMOTE",
                "group_id": "payment",
                "topic": "orders",
                "partition": 1,
                "generation": 1  # Stale!
            }, sock_stale)
            resp_stale = sock_stale.get_last_response()
            self.assertEqual(resp_stale["type"], "ERROR")
            self.assertEqual(resp_stale["payload"]["code"], "STALE_GENERATION")

            # 3. Wrong consumer request to Broker 2 -> rejected after querying Coordinator over real TCP
            sock_wrong = MockSocket()
            handle_consume({
                "version": 1,
                "type": MESSAGE_TYPE_CONSUME,
                "consumer_id": "C_IMPOSTER",
                "group_id": "payment",
                "topic": "orders",
                "partition": 1,
                "generation": 2
            }, sock_wrong)
            resp_wrong = sock_wrong.get_last_response()
            self.assertEqual(resp_wrong["type"], "ERROR")
            self.assertEqual(resp_wrong["payload"]["code"], "CONSUMER_NOT_REGISTERED")

        finally:
            coord_running[0] = False
            coord_server.close()
            t.join(timeout=1.0)
            KNOWN_BROKERS.clear()
            KNOWN_BROKERS.update(orig_known)

    def test_10_ack_fencing_behavior(self):
        """Verifies that an ACK from a stale consumer or mismatched generation is rejected."""
        broker_id["value"] = 1
        # In-flight delivery for C1 with generation 2
        in_flight["payment"] = {
            701: {
                "message": {"topic": "orders", "partition": 0, "message_id": 701},
                "offset": 0,
                "consumer_id": "C1",
                "generation": 2
            }
        }

        # Attempt to ACK by a different consumer C2 -> rejected
        handle_ack({
            "version": 1,
            "type": MESSAGE_TYPE_ACK,
            "group_id": "payment",
            "consumer_id": "C2",  # Mismatch!
            "message_id": 701,
            "topic": "orders",
            "partition": 0,
            "generation": 2
        })
        self.assertIn(701, in_flight["payment"])
        self.assertNotIn("payment", consumer_offsets)

        # Attempt to ACK with stale generation 1 -> rejected
        handle_ack({
            "version": 1,
            "type": MESSAGE_TYPE_ACK,
            "group_id": "payment",
            "consumer_id": "C1",
            "message_id": 701,
            "topic": "orders",
            "partition": 0,
            "generation": 1  # Stale generation!
        })
        self.assertIn(701, in_flight["payment"])
        self.assertNotIn("payment", consumer_offsets)

        # Valid ACK with matching consumer_id and generation -> accepted
        handle_ack({
            "version": 1,
            "type": MESSAGE_TYPE_ACK,
            "group_id": "payment",
            "consumer_id": "C1",
            "message_id": 701,
            "topic": "orders",
            "partition": 0,
            "generation": 2
        })
        self.assertNotIn(701, in_flight.get("payment", {}))
        self.assertEqual(consumer_offsets.get("payment", {}).get("orders", {}).get(0), 1)


if __name__ == "__main__":
    unittest.main()
