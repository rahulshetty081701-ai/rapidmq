import unittest
from broker.storage import consumers, partition_assignments, topics
from broker.rebalance import rebalance
from broker.partition import Partition
from broker.metadata import get_topic_partitions

class TestClusterRebalance(unittest.TestCase):
    def setUp(self):
        consumers.clear()
        partition_assignments.clear()
        topics.clear()

    def test_rebalance_with_partial_local_partitions(self):
        # Scenario 1: Coordinator only has partition 0 locally for topic "orders"
        # Remote brokers own partition 1 and 2.
        topics["orders"] = {
            0: Partition(0)
        }

        # Register 2 active consumers
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}
        consumers["C2"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}

        # Run rebalance
        assignment = rebalance("payment", "orders")

        # Verify cluster-wide configured partitions [0, 1, 2] were assigned
        self.assertEqual(get_topic_partitions("orders"), [0, 1, 2])
        self.assertEqual(assignment["C1"], [0, 2])
        self.assertEqual(assignment["C2"], [1])

        # Verify state in consumers and partition_assignments
        self.assertEqual(consumers["C1"]["partitions"], [0, 2])
        self.assertEqual(consumers["C2"]["partitions"], [1])
        self.assertEqual(partition_assignments["payment"]["orders"][0], "C1")
        self.assertEqual(partition_assignments["payment"]["orders"][1], "C2")
        self.assertEqual(partition_assignments["payment"]["orders"][2], "C1")

        # Verify rebalance did NOT create local copies of remote partitions in topics dictionary!
        self.assertEqual(list(topics["orders"].keys()), [0])
        self.assertNotIn(1, topics["orders"])
        self.assertNotIn(2, topics["orders"])

    def test_rebalance_with_zero_local_partitions(self):
        # Scenario: Topic has not been published to locally on coordinator at all (topics is empty)
        # But consumers register for topic "orders"
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}
        assignment = rebalance("payment", "orders")

        self.assertEqual(assignment["C1"], [0, 1, 2])
        self.assertEqual(consumers["C1"]["partitions"], [0, 1, 2])
        # Verify local topics dictionary remains untouched
        self.assertNotIn("orders", topics)

    def test_deterministic_and_balanced_distribution(self):
        # 3 consumers across 3 partitions
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}
        consumers["C2"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}
        consumers["C3"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}

        assignment = rebalance("payment", "orders")
        self.assertEqual(assignment["C1"], [0])
        self.assertEqual(assignment["C2"], [1])
        self.assertEqual(assignment["C3"], [2])

    def test_existing_single_broker_regression(self):
        # All 3 partitions exist locally
        topics["orders"] = {
            0: Partition(0),
            1: Partition(1),
            2: Partition(2)
        }
        consumers["C1"] = {"group_id": "payment", "topic": "orders", "status": "ACTIVE", "partitions": []}
        assignment = rebalance("payment", "orders")
        self.assertEqual(assignment["C1"], [0, 1, 2])
        self.assertEqual(len(topics["orders"]), 3)

if __name__ == "__main__":
    unittest.main()
