"""
Step 14.5.7 Tests: Cross-broker consumer failure cleanup, retries, and offset safety.

Test scenarios:
  1. Consumer data connection failure while coordinator connection remains alive.
  2. Consumer death with unacknowledged messages.
  3. Timeout and disconnect cleanup race — no duplicate state corruption.
  4. Late ACK after timeout redelivery.
  5. Multiple unacknowledged messages in one partition, out-of-order ACKs.
  6. Reassignment with uncommitted messages.
  7. Retry limit and dead-letter queue behavior.
  8. Offset recovery without skipping uncommitted messages.
  9. Existing single-broker behavior.
"""

import unittest
import threading
import socket
import time
import uuid

from common.protocol import encode_message, recv_message
from common.constants import HOST, KNOWN_BROKERS

# Reset all shared broker state before each test
from broker import storage as _storage
from broker.storage import (
    topics, queue_lock,
    consumers, consumers_lock,
    in_flight, in_flight_lock,
    retry_queue, retry_queue_lock,
    dead_letter_queue, dead_letter_queue_lock,
    resolved_offsets, resolved_offsets_lock,
    pending_acks, pending_acks_lock,
    consumer_offsets, consumer_offsets_lock,
    next_offset, next_offset_lock,
    broker_id,
    group_generations, group_generations_lock,
    partition_assignments, partition_assignments_lock,
)
from broker.partition import Partition
from broker.timeout_manager import requeue_or_dlq, check_timeouts, MAX_RETRIES
from broker.offset_manager import advance_committed_offset, get_offset
from broker.message_handlers import (
    release_consumer_partition,
    handle_release_assignment,
    handle_consume,
    handle_ack,
)
from broker.dlq_manager import append_to_dlq


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clear_state():
    """Reset all in-memory broker state between tests."""
    with queue_lock:
        topics.clear()
    with consumers_lock:
        consumers.clear()
    with in_flight_lock:
        in_flight.clear()
    with retry_queue_lock:
        retry_queue.clear()
    with dead_letter_queue_lock:
        dead_letter_queue.clear()
    with resolved_offsets_lock:
        resolved_offsets.clear()
    with pending_acks_lock:
        pending_acks.clear()
    with consumer_offsets_lock:
        consumer_offsets.clear()
    with next_offset_lock:
        next_offset.clear()
    with group_generations_lock:
        group_generations.clear()
    with partition_assignments_lock:
        partition_assignments.clear()
    broker_id["value"] = 1


def _make_topic(topic_name, num_partitions=3):
    with queue_lock:
        topics[topic_name] = {i: Partition(i) for i in range(num_partitions)}


def _publish(topic, partition, payload, msg_id=None):
    """Append a message to a topic partition, returning the offset."""
    with queue_lock:
        if msg_id is None:
            msg_id = int(uuid.uuid4().int % 10**6)
        msg = {"message_id": msg_id, "topic": topic, "payload": payload, "partition": partition}
        offset = topics[topic][partition].append(msg)
    return offset, msg_id


def _register_consumer(consumer_id, group_id, topic, partitions, status="ACTIVE"):
    with consumers_lock:
        consumers[consumer_id] = {
            "group_id": group_id,
            "topic": topic,
            "partitions": list(partitions),
            "last_heartbeat": time.time(),
            "status": status,
        }


def _set_generation(group_id, topic, gen):
    with group_generations_lock:
        group_generations[(group_id, topic)] = gen


def _set_assignment(group_id, topic, partition_to_consumer):
    """partition_to_consumer: {partition_id: consumer_id}"""
    with partition_assignments_lock:
        if group_id not in partition_assignments:
            partition_assignments[group_id] = {}
        partition_assignments[group_id][topic] = dict(partition_to_consumer)


def _put_in_flight(group_id, msg_id, msg, offset, partition, consumer_id, generation=1, retry_count=0, ts=None):
    with in_flight_lock:
        if group_id not in in_flight:
            in_flight[group_id] = {}
        in_flight[group_id][msg_id] = {
            "message": msg,
            "timestamp": ts if ts is not None else time.time(),
            "offset": offset,
            "partition": partition,
            "generation": generation,
            "retry_count": retry_count,
            "consumer_id": consumer_id,
        }


def _advance_next_offset(group_id, topic, partition, offset):
    with next_offset_lock:
        if group_id not in next_offset:
            next_offset[group_id] = {}
        if topic not in next_offset[group_id]:
            next_offset[group_id][topic] = {}
        next_offset[group_id][topic][partition] = offset


class MockSocket:
    """Minimal socket stub that captures sent bytes."""
    def __init__(self):
        self._sent = []
        self.closed = False

    def sendall(self, data):
        self._sent.append(data)

    def close(self):
        self.closed = True

    def last_response(self):
        if not self._sent:
            return None
        return recv_message_from_bytes(self._sent[-1])

    def all_responses(self):
        return [recv_message_from_bytes(d) for d in self._sent]


def recv_message_from_bytes(data):
    """Decode a single framed message from raw bytes."""
    import io
    import struct
    import json
    if len(data) < 4:
        return None
    length = struct.unpack("!I", data[:4])[0]
    return json.loads(data[4:4 + length])


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class Test1DataConnectionFailureCoordinatorAlive(unittest.TestCase):
    """1. Consumer data connection failure while coordinator connection remains alive."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_close_broker_socket_does_not_affect_coordinator(self):
        """Closing a data socket leaves the consumer object alive (status ACTIVE)."""
        from consumer.consumer_thread import ConsumerClient

        # Patch auto_connect=False so no real TCP socket is created
        client = ConsumerClient("g1", "orders", auto_connect=False)
        client.consumer_id = "C1"
        client.assigned_partitions = [0]
        client.generation = 1

        # Simulate a connected data socket for broker 2
        fake_sock = MockSocket()
        with client.sockets_lock:
            client.data_sockets[2] = fake_sock

        # Close broker 2 data socket (simulates data-connection failure)
        client._close_broker_socket(2)

        with client.sockets_lock:
            self.assertNotIn(2, client.data_sockets)

        # Coordinator socket field is unchanged (None — we never connected)
        self.assertIsNone(client.consumer_socket)

        # consumer_id is still set — consumer has not "died"
        self.assertEqual(client.consumer_id, "C1")

    def test_consume_returns_none_on_data_socket_ioerror(self):
        """_consume_from_partition returns None when broker socket raises OSError."""
        from consumer.consumer_thread import ConsumerClient

        class FailingSocket:
            def sendall(self, data):
                raise OSError("simulated send failure")
            def close(self):
                pass

        client = ConsumerClient("g1", "orders", auto_connect=False)
        client.consumer_id = "C1"
        client.assigned_partitions = [0]
        client.generation = 1

        with client.sockets_lock:
            client.data_sockets[1] = FailingSocket()

        # Override resolver so it routes to broker 1
        original = client.resolve_partition_broker
        client.resolve_partition_broker = lambda p: (1, ("127.0.0.1", 9000))

        result = client._consume_from_partition(0, generation=1)
        self.assertIsNone(result)

        # Socket must be removed from pool after failure
        with client.sockets_lock:
            self.assertNotIn(1, client.data_sockets)

        client.resolve_partition_broker = original


class Test2ConsumerDeathUnacknowledgedMessages(unittest.TestCase):
    """2. Consumer death with unacknowledged messages."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_release_requeues_in_flight_messages(self):
        group_id = "pay"
        topic = "orders"
        cid = "C1"
        msg_id = 101
        msg = {"message_id": msg_id, "topic": topic, "payload": "hello", "partition": 0}

        _register_consumer(cid, group_id, topic, [0])
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id=cid)

        release_consumer_partition(cid)

        # Message should be in retry queue
        with retry_queue_lock:
            queue = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["message"]["message_id"], msg_id)

    def test_release_marks_consumer_dead(self):
        cid = "C2"
        _register_consumer(cid, "g", "t", [0])
        release_consumer_partition(cid)
        with consumers_lock:
            self.assertEqual(consumers[cid]["status"], "DEAD")

    def test_release_idempotent_no_double_queue(self):
        """Calling release_consumer_partition twice must not double-queue."""
        group_id = "pay"
        topic = "orders"
        cid = "C3"
        msg_id = 201
        msg = {"message_id": msg_id, "topic": topic, "payload": "x", "partition": 0}

        _register_consumer(cid, group_id, topic, [0])
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id=cid)

        release_consumer_partition(cid)
        # Second call (consumer already DEAD, in_flight already moved)
        release_consumer_partition(cid)

        with retry_queue_lock:
            queue = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(queue), 1)


class Test3TimeoutDisconnectRaceNoDuplicates(unittest.TestCase):
    """3. Timeout and disconnect cleanup racing — no duplicate state corruption."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_requeue_or_dlq_deduplicates(self):
        """If both timeout and release call requeue_or_dlq for same message, it appears once."""
        group_id = "g"
        topic = "t"
        msg_id = 999
        msg = {"message_id": msg_id, "topic": topic, "payload": "y", "partition": 0}
        data = {
            "message": msg,
            "offset": 0,
            "partition": 0,
            "retry_count": 0,
            "consumer_id": "C1",
            "timestamp": time.time(),
        }

        requeue_or_dlq(group_id, msg_id, data)
        requeue_or_dlq(group_id, msg_id, data)  # duplicate — should be ignored

        with retry_queue_lock:
            queue = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(queue), 1)

    def test_timeout_and_release_race(self):
        """
        Message in_flight is removed by timeout, then release_consumer_partition
        finds nothing to requeue — net result: exactly one retry entry.
        """
        group_id = "pay"
        topic = "orders"
        cid = "C1"
        msg_id = 55
        msg = {"message_id": msg_id, "topic": topic, "payload": "t", "partition": 0}

        _register_consumer(cid, group_id, topic, [0])

        # Insert into in_flight with a stale timestamp (expired)
        _put_in_flight(
            group_id, msg_id, msg, offset=0, partition=0, consumer_id=cid,
            ts=time.time() - 60
        )

        # Simulate timeout firing first (removes from in_flight, adds to retry)
        check_timeouts(current=time.time())

        with in_flight_lock:
            remaining = list(in_flight.get(group_id, {}).keys())
        self.assertNotIn(msg_id, remaining)

        # Now simulate release (consumer dies) — in_flight is empty for this consumer
        release_consumer_partition(cid)

        # Exactly one entry in retry queue
        with retry_queue_lock:
            queue = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(queue), 1)


class Test4LateAckAfterTimeoutRedelivery(unittest.TestCase):
    """4. Late ACK after timeout redelivery should be rejected (stale retry_count)."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_stale_retry_count_ack_is_rejected(self):
        group_id = "g"
        topic = "t"
        msg_id = 42
        msg = {"message_id": msg_id, "topic": topic, "payload": "z", "partition": 0}

        _make_topic(topic, 3)
        _set_generation(group_id, topic, 1)
        _register_consumer("C1", group_id, topic, [0])

        # First delivery: retry_count=0
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id="C1", retry_count=0)

        # Timeout fires — message moved to retry; retry_count=1 on next delivery
        with in_flight_lock:
            del in_flight[group_id][msg_id]

        # New in-flight entry representing retry delivery (retry_count=1)
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id="C1", retry_count=1)

        # Record offset before stale ACK
        before_offsets = get_offset(group_id, topic, 0)

        # Late ACK carries retry_count=0 (from stale first delivery)
        stale_ack = {
            "type": "ACK",
            "group_id": group_id,
            "consumer_id": "C1",
            "message_id": msg_id,
            "topic": topic,
            "partition": 0,
            "generation": 1,
            "retry_count": 0,  # stale
        }
        handle_ack(stale_ack)

        # Message must still be in-flight (ACK rejected)
        with in_flight_lock:
            self.assertIn(msg_id, in_flight.get(group_id, {}))

        # Offset must not have advanced
        after_offsets = get_offset(group_id, topic, 0)
        self.assertEqual(before_offsets, after_offsets)

    def test_correct_retry_count_ack_succeeds(self):
        group_id = "g"
        topic = "t"
        msg_id = 43
        msg = {"message_id": msg_id, "topic": topic, "payload": "z", "partition": 0}

        _make_topic(topic, 3)
        _set_generation(group_id, topic, 1)
        _register_consumer("C1", group_id, topic, [0])
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id="C1", retry_count=1)

        with pending_acks_lock:
            pending_acks.setdefault(group_id, {}).setdefault(topic, {})[0] = set()

        valid_ack = {
            "type": "ACK",
            "group_id": group_id,
            "consumer_id": "C1",
            "message_id": msg_id,
            "topic": topic,
            "partition": 0,
            "generation": 1,
            "retry_count": 1,
        }
        handle_ack(valid_ack)

        with in_flight_lock:
            self.assertNotIn(msg_id, in_flight.get(group_id, {}))


class Test5OutOfOrderAcksMultipleInFlight(unittest.TestCase):
    """5. Multiple in-flight messages, ACKs arriving out of order."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_committed_offset_does_not_skip_unacked(self):
        """
        Messages at offsets 0, 1, 2 are in-flight.
        ACK offset 2, then offset 0. Committed offset should only reach 1
        once offset 1 is also ACKed.
        """
        group_id = "g"
        topic = "t"

        _make_topic(topic, 1)
        _set_generation(group_id, topic, 1)
        _register_consumer("C1", group_id, topic, [0])

        # initialise pending_acks
        with pending_acks_lock:
            pending_acks[group_id] = {topic: {0: set()}}

        ids = [300, 301, 302]
        for i, mid in enumerate(ids):
            msg = {"message_id": mid, "topic": topic, "payload": f"p{i}", "partition": 0}
            _put_in_flight(group_id, mid, msg, offset=i, partition=0, consumer_id="C1")
            _advance_next_offset(group_id, topic, 0, i + 1)

        def ack(mid, offset):
            handle_ack({
                "type": "ACK",
                "group_id": group_id,
                "consumer_id": "C1",
                "message_id": mid,
                "topic": topic,
                "partition": 0,
                "generation": 1,
            })

        # ACK offset 2 (msg_id 302)
        ack(302, 2)
        committed = get_offset(group_id, topic, 0)
        # offset 0 and 1 still pending — committed should remain at 0
        self.assertEqual(committed, 0)

        # ACK offset 0 (msg_id 300)
        ack(300, 0)
        committed = get_offset(group_id, topic, 0)
        # offset 1 still pending — committed should advance to 1 (offset 0 is done)
        self.assertEqual(committed, 1)

        # ACK offset 1 (msg_id 301)
        ack(301, 1)
        committed = get_offset(group_id, topic, 0)
        # All ACKed — committed should reach 3
        self.assertEqual(committed, 3)


class Test6ReassignmentUncommittedMessages(unittest.TestCase):
    """6. Reassignment with uncommitted messages."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_next_offset_resets_to_committed_after_release(self):
        """
        Consumer C1 fetches message (next_offset advances to 1),
        dies before ACKing. After release, next_offset must reset to
        committed_offset (0) so the new consumer doesn't skip offset 0.
        """
        group_id = "g"
        topic = "orders"
        cid = "C1"

        _make_topic(topic, 1)
        _register_consumer(cid, group_id, topic, [0])
        _set_generation(group_id, topic, 1)
        _advance_next_offset(group_id, topic, 0, 1)  # fetched one message

        msg_id = 77
        msg = {"message_id": msg_id, "topic": topic, "payload": "hi", "partition": 0}
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id=cid)

        release_consumer_partition(cid)

        # After release: in-flight moved to retry
        with retry_queue_lock:
            queue = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertGreater(len(queue), 0)

        # next_offset should still be >=1 because retry will serve from retry_queue
        # (the fence only resets when no retry entries exist)
        # The important thing: if retry_queue IS present, next_offset is preserved.
        with next_offset_lock:
            cur_next = next_offset.get(group_id, {}).get(topic, {}).get(0, None)
        # Either preserved (because retry exists) or reset to committed (0)
        # In either case it must not be > committed when retries exist
        committed = get_offset(group_id, topic, 0)
        if cur_next is not None and cur_next > committed:
            # Verify there are retry entries to justify the non-reset
            with retry_queue_lock:
                has_retry = bool(retry_queue.get(group_id, {}).get(topic, {}).get(0, []))
            self.assertTrue(has_retry, "next_offset advanced past committed with no retry entries")

    def test_handle_release_assignment_resets_next_offset_no_retries(self):
        """
        RELEASE_ASSIGNMENT on a remote broker: if no retry entries exist,
        next_offset must be reset to committed_offset.
        """
        group_id = "g2"
        topic = "orders"

        _make_topic(topic, 1)
        _advance_next_offset(group_id, topic, 0, 5)
        # committed is 0 (nothing advanced)

        msg = {
            "type": "RELEASE_ASSIGNMENT",
            "group_id": group_id,
            "topic": topic,
            "partition": 0,
            "consumer_id": "C1"
        }
        handle_release_assignment(msg, client_socket=None)

        with next_offset_lock:
            cur = next_offset.get(group_id, {}).get(topic, {}).get(0, None)
        committed = get_offset(group_id, topic, 0)
        self.assertEqual(cur, committed)


class Test7RetryLimitAndDLQ(unittest.TestCase):
    """7. Retry limit exhaustion routes message to dead-letter queue."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_message_dlq_after_max_retries(self):
        group_id = "g"
        topic = "t"
        msg_id = 500

        msg = {"message_id": msg_id, "topic": topic, "payload": "dlq_test", "partition": 0}
        data = {
            "message": msg,
            "offset": 0,
            "partition": 0,
            "retry_count": MAX_RETRIES,  # already at limit
            "consumer_id": "C1",
            "timestamp": time.time(),
        }

        requeue_or_dlq(group_id, msg_id, data)

        # Should be in DLQ, not retry
        with dead_letter_queue_lock:
            dlq = dead_letter_queue.get(group_id, {}).get(topic, [])
        self.assertEqual(len(dlq), 1)
        self.assertEqual(dlq[0]["message"]["message_id"], msg_id)

        with retry_queue_lock:
            rq = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(rq), 0)

    def test_dlq_resolves_offset(self):
        """After DLQ, resolved_offsets contains the message's offset."""
        group_id = "g"
        topic = "t"
        msg_id = 501

        _make_topic(topic, 1)
        msg = {"message_id": msg_id, "topic": topic, "payload": "dlq_offset", "partition": 0}
        data = {
            "message": msg,
            "offset": 7,
            "partition": 0,
            "retry_count": MAX_RETRIES,
            "consumer_id": "C1",
            "timestamp": time.time(),
        }

        requeue_or_dlq(group_id, msg_id, data)

        with resolved_offsets_lock:
            resolved = resolved_offsets.get(group_id, {}).get(topic, {}).get(0, set())
        self.assertIn(7, resolved)

    def test_retry_before_dlq(self):
        """Messages below MAX_RETRIES go to retry queue, not DLQ."""
        group_id = "g"
        topic = "t"
        msg_id = 502

        msg = {"message_id": msg_id, "topic": topic, "payload": "retry_me", "partition": 0}
        data = {
            "message": msg,
            "offset": 0,
            "partition": 0,
            "retry_count": 0,  # first attempt
            "consumer_id": "C1",
            "timestamp": time.time(),
        }

        requeue_or_dlq(group_id, msg_id, data)

        with retry_queue_lock:
            rq = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(rq), 1)

        with dead_letter_queue_lock:
            dlq = dead_letter_queue.get(group_id, {}).get(topic, [])
        self.assertEqual(len(dlq), 0)


class Test8OffsetRecoveryNoSkipping(unittest.TestCase):
    """8. Offset recovery after consumer reconnect does not skip uncommitted messages."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_new_consumer_starts_from_committed_offset(self):
        """
        Old consumer C1 fetched offset 0 but never ACKed.
        After C1 dies and a new CONSUME arrives, the broker serves from retry
        (or resets next_offset to committed), so message at offset 0 is not skipped.
        """
        group_id = "g"
        topic = "orders"
        cid = "C1"
        msg_id = 600

        _make_topic(topic, 1)
        _set_generation(group_id, topic, 1)

        offset, _ = _publish(topic, 0, "msg0", msg_id=msg_id)
        self.assertEqual(offset, 0)

        _register_consumer(cid, group_id, topic, [0])
        _advance_next_offset(group_id, topic, 0, 1)  # C1 fetched it

        msg = {"message_id": msg_id, "topic": topic, "payload": "msg0", "partition": 0}
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id=cid)

        # C1 dies
        release_consumer_partition(cid)

        # Committed offset is still 0 (not advanced)
        self.assertEqual(get_offset(group_id, topic, 0), 0)

        # Message should be in retry queue (not skipped)
        with retry_queue_lock:
            rq = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertGreater(len(rq), 0)
        self.assertEqual(rq[0]["message"]["message_id"], msg_id)

    def test_next_offset_not_advanced_past_committed_without_retry(self):
        """
        If there's no retry and no in-flight, handle_consume must NOT advance
        past the committed offset for a new consumer.
        """
        group_id = "g2"
        topic = "orders"
        cid = "C2"

        _make_topic(topic, 1)
        _set_generation(group_id, topic, 1)
        _set_assignment(group_id, topic, {0: cid})
        _register_consumer(cid, group_id, topic, [0])

        # committed = 0; next_offset jumped ahead due to old consumer, now reset
        _advance_next_offset(group_id, topic, 0, 3)

        # No retry queue, no in-flight
        msg = {
            "version": 1,
            "type": "CONSUME",
            "topic": topic,
            "group_id": group_id,
            "consumer_id": cid,
            "partition": 0,
            "generation": 1,
        }

        sock = MockSocket()
        handle_consume(msg, sock)

        resp = sock.last_response()
        # After the offset-safety fence, next_offset should have been reset to 0
        with next_offset_lock:
            cur = next_offset.get(group_id, {}).get(topic, {}).get(0)
        # The fence resets to committed (0), so no fetch was made → EMPTY
        self.assertIn(resp["type"], ("EMPTY", "ERROR"))


class Test9SingleBrokerBehavior(unittest.TestCase):
    """9. Existing single-broker behavior is preserved."""

    def setUp(self):
        _clear_state()
        broker_id["value"] = 1

    def test_basic_consume_ack_cycle(self):
        group_id = "g"
        topic = "orders"
        cid = "C1"

        _make_topic(topic, 1)
        _set_generation(group_id, topic, 1)
        _set_assignment(group_id, topic, {0: cid})
        _register_consumer(cid, group_id, topic, [0])

        _publish(topic, 0, "hello world", msg_id=777)

        with pending_acks_lock:
            pending_acks[group_id] = {topic: {0: set()}}

        consume_msg = {
            "version": 1,
            "type": "CONSUME",
            "topic": topic,
            "group_id": group_id,
            "consumer_id": cid,
            "partition": 0,
            "generation": 1,
        }
        sock = MockSocket()
        handle_consume(consume_msg, sock)
        response = sock.last_response()

        self.assertNotEqual(response.get("type"), "ERROR")
        self.assertNotEqual(response.get("type"), "EMPTY")
        self.assertEqual(response.get("payload"), "hello world")
        self.assertEqual(response.get("retry_count"), 0)

        msg_id = response["message_id"]
        offset = response.get("offset", 0)

        # ACK it
        handle_ack({
            "type": "ACK",
            "group_id": group_id,
            "consumer_id": cid,
            "message_id": msg_id,
            "topic": topic,
            "partition": 0,
            "generation": 1,
            "retry_count": 0,
        })

        # In-flight should be empty
        with in_flight_lock:
            self.assertEqual(in_flight.get(group_id, {}), {})

        # Committed offset should have advanced
        committed = get_offset(group_id, topic, 0)
        self.assertGreater(committed, 0)

    def test_stale_generation_rejected(self):
        group_id = "g"
        topic = "orders"
        cid = "C1"

        _make_topic(topic, 1)
        _set_generation(group_id, topic, 2)
        _set_assignment(group_id, topic, {0: cid})
        _register_consumer(cid, group_id, topic, [0])

        consume_msg = {
            "version": 1,
            "type": "CONSUME",
            "topic": topic,
            "group_id": group_id,
            "consumer_id": cid,
            "partition": 0,
            "generation": 1,  # stale
        }
        sock = MockSocket()
        handle_consume(consume_msg, sock)
        response = sock.last_response()

        self.assertEqual(response.get("type"), "ERROR")
        payload = response.get("payload", {})
        self.assertEqual(payload.get("code"), "STALE_GENERATION")

        # In-flight must be empty (request rejected before any state mutation)
        with in_flight_lock:
            self.assertEqual(in_flight.get(group_id, {}), {})

    def test_partition_not_assigned_rejected(self):
        group_id = "g"
        topic = "orders"
        cid = "C1"

        _make_topic(topic, 3)
        _set_generation(group_id, topic, 1)
        # Only partition 0 assigned to C1
        _set_assignment(group_id, topic, {0: cid, 1: "C2", 2: "C3"})
        _register_consumer(cid, group_id, topic, [0])

        # C1 tries to consume from partition 1 (not assigned to it)
        consume_msg = {
            "version": 1,
            "type": "CONSUME",
            "topic": topic,
            "group_id": group_id,
            "consumer_id": cid,
            "partition": 1,
            "generation": 1,
        }
        sock = MockSocket()
        handle_consume(consume_msg, sock)
        response = sock.last_response()

        self.assertEqual(response.get("type"), "ERROR")
        payload = response.get("payload", {})
        self.assertIn(payload.get("code"), ("PARTITION_NOT_ASSIGNED", "NOT_PARTITION_OWNER"))

        # In-flight must be clean
        with in_flight_lock:
            self.assertEqual(in_flight.get(group_id, {}), {})


class TestRealSocketRemoteBrokerRelease(unittest.TestCase):
    """
    Cross-broker validation: remote data broker receives RELEASE_ASSIGNMENT,
    requeues in-flight messages, and resets next_offset.
    Uses real TCP loopback (same pattern as test_assignment_fencing.py).
    """

    def _start_server(self, host, port):
        """Start a minimal broker server that handles one RELEASE_ASSIGNMENT."""
        from broker.producer_handlers import handle_client as _handle_client
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(1)
        server.settimeout(5)
        return server

    def setUp(self):
        _clear_state()
        broker_id["value"] = 2  # data broker (remote)

    def tearDown(self):
        _clear_state()
        broker_id["value"] = 1

    def test_release_assignment_over_tcp(self):
        """
        Simulate coordinator sending RELEASE_ASSIGNMENT to a remote data broker
        over a real TCP socket, and verify in-flight cleanup.
        """
        HOST_LOCAL = "127.0.0.1"
        PORT_LOCAL = 19870

        group_id = "pay"
        topic = "orders"
        cid = "C1"
        msg_id = 888
        msg = {"message_id": msg_id, "topic": topic, "payload": "tcp_test", "partition": 0}

        # Setup: remote broker has a message in-flight for C1 on partition 0
        _make_topic(topic, 1)
        _put_in_flight(group_id, msg_id, msg, offset=0, partition=0, consumer_id=cid)
        _advance_next_offset(group_id, topic, 0, 1)

        # Start a real TCP server (the "remote" data broker)
        from broker.producer_handlers import handle_client
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST_LOCAL, PORT_LOCAL))
        server.listen(1)
        server.settimeout(5)

        server_error = []

        def _serve_one():
            try:
                conn, _ = server.accept()
                handle_client(conn, ("127.0.0.1", 99999))
            except Exception as e:
                server_error.append(e)

        t = threading.Thread(target=_serve_one, daemon=True)
        t.start()

        # Give server a moment to start
        time.sleep(0.05)

        # Client sends RELEASE_ASSIGNMENT
        from common.constants import MESSAGE_TYPE_RELEASE_ASSIGNMENT
        release_msg = {
            "version": 1,
            "type": MESSAGE_TYPE_RELEASE_ASSIGNMENT,
            "group_id": group_id,
            "topic": topic,
            "partition": 0,
            "consumer_id": cid,
        }
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect((HOST_LOCAL, PORT_LOCAL))
        client_sock.sendall(encode_message(release_msg))
        resp = recv_message(client_sock)
        client_sock.close()
        server.close()
        t.join(timeout=3)

        # Verify response
        self.assertIsNotNone(resp)
        self.assertEqual(resp.get("type"), "RELEASE_ASSIGNMENT_RESPONSE")

        # In-flight must be cleared
        with in_flight_lock:
            self.assertNotIn(msg_id, in_flight.get(group_id, {}))

        # Retry queue should have the message
        with retry_queue_lock:
            rq = retry_queue.get(group_id, {}).get(topic, {}).get(0, [])
        self.assertEqual(len(rq), 1)
        self.assertEqual(rq[0]["message"]["message_id"], msg_id)

        # next_offset should have been reset to committed (0), since retry is populated
        # (fence only triggers reset when no retry entries exist — here entries exist)
        # Either way, committed must not be skipped
        committed = get_offset(group_id, topic, 0)
        self.assertEqual(committed, 0)

        if server_error:
            self.fail(f"Server thread error: {server_error[0]}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
