from broker.storage import consumers, partition_assignments, topics
from broker.rebalance import rebalance
from broker.partition import Partition

# Mock 3 partitions for topic "order"
topics["order"] = {
    0: Partition(0),
    1: Partition(1),
    2: Partition(2)
}

def print_state(test_name):
    print(f"\n--- {test_name} ---")
    rebalance("payment", "order")
    print("Consumers State:")
    for cid, cinfo in sorted(consumers.items()):
        if cinfo["group_id"] == "payment" and cinfo["topic"] == "order":
            print(f"{cid} -> {cinfo['partitions']}")
    print("Partition Assignments State:")
    if "payment" in partition_assignments and "order" in partition_assignments["payment"]:
        for pid, cid in sorted(partition_assignments["payment"]["order"].items()):
            print(f"P{pid} -> {cid}")

# Test 1: 3 partitions, 1 consumer
consumers.clear()
partition_assignments.clear()
consumers["C1"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
print_state("Test 1: 3 partitions, 1 consumer")

# Test 2: 3 partitions, 2 consumers
consumers.clear()
partition_assignments.clear()
consumers["C1"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C2"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
print_state("Test 2: 3 partitions, 2 consumers")

# Test 3: 3 partitions, 3 consumers
consumers.clear()
partition_assignments.clear()
consumers["C1"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C2"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C3"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
print_state("Test 3: 3 partitions, 3 consumers")

# Test 4: 3 partitions, 4 consumers
consumers.clear()
partition_assignments.clear()
consumers["C1"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C2"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C3"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C4"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
print_state("Test 4: 3 partitions, 4 consumers")

# Test 5: Verify consumer from another group/topic is not included
consumers.clear()
partition_assignments.clear()
consumers["C1"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["C2"] = {"group_id": "payment", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["OTHER1"] = {"group_id": "other_group", "topic": "order", "status": "ACTIVE", "partitions": []}
consumers["OTHER2"] = {"group_id": "payment", "topic": "other_topic", "status": "ACTIVE", "partitions": []}
consumers["DEAD"] = {"group_id": "payment", "topic": "order", "status": "DEAD", "partitions": []}
print_state("Test 5: Exclude other consumers/topics/groups")

print("\nOther Consumers State after Test 5:")
print(f"OTHER1: {consumers['OTHER1']['partitions']}")
print(f"OTHER2: {consumers['OTHER2']['partitions']}")
print(f"DEAD: {consumers['DEAD']['partitions']}")
