from broker.storage import consumers, topics, next_offset
from broker.partition import Partition
from broker.scheduler import select_partition, next_partition_index

group_id = "test_group"
topic = "test_topic"
cid = "consumer_1"

# Setup basic state
consumers[cid] = {
    "group_id": group_id,
    "topic": topic,
    "partitions": [],
    "status": "ACTIVE"
}
topics[topic] = {
    0: Partition(0),
    1: Partition(1),
    2: Partition(2)
}
next_offset[group_id] = {topic: {0: 0, 1: 0, 2: 0}}

def add_message(part_id, msg):
    topics[topic][part_id].messages.append(msg)

def reset_test(partitions):
    consumers[cid]["partitions"] = partitions
    next_partition_index[cid] = 0
    topics[topic] = {0: Partition(0), 1: Partition(1), 2: Partition(2)}
    next_offset[group_id] = {topic: {0: 0, 1: 0, 2: 0}}

print("--- Test 1: Two assigned partitions, both containing messages ---")
reset_test([0, 1])
add_message(0, "msgA")
add_message(1, "msgB")
add_message(0, "msgC")
add_message(1, "msgD")
print(f"Call 1: {select_partition(cid)} (expected 0)")
next_offset[group_id][topic][0] += 1
print(f"Call 2: {select_partition(cid)} (expected 1)")
next_offset[group_id][topic][1] += 1
print(f"Call 3: {select_partition(cid)} (expected 0)")
next_offset[group_id][topic][0] += 1
print(f"Call 4: {select_partition(cid)} (expected 1)")
next_offset[group_id][topic][1] += 1

print("\n--- Test 2: Two assigned partitions, only P1 has messages ---")
reset_test([0, 1])
add_message(1, "msgB")
print(f"Call 1: {select_partition(cid)} (expected 1)")

print("\n--- Test 3: P0 initially empty, P1 has messages, then P0 receives messages ---")
reset_test([0, 1])
add_message(1, "msgB")
print(f"Call 1: {select_partition(cid)} (expected 1)")
next_offset[group_id][topic][1] += 1
add_message(0, "msgA")
add_message(1, "msgB2")
print(f"Call 2: {select_partition(cid)} (expected 0)")
next_offset[group_id][topic][0] += 1
print(f"Call 3: {select_partition(cid)} (expected 1)")

print("\n--- Test 4: Three assigned partitions ---")
reset_test([0, 1, 2])
add_message(0, "msgA")
add_message(1, "msgB")
add_message(2, "msgC")
print(f"Call 1: {select_partition(cid)} (expected 0)")
next_offset[group_id][topic][0] += 1
print(f"Call 2: {select_partition(cid)} (expected 1)")
next_offset[group_id][topic][1] += 1
print(f"Call 3: {select_partition(cid)} (expected 2)")
next_offset[group_id][topic][2] += 1

print("\n--- Test 5: Consumer with only one assigned partition ---")
reset_test([2])
add_message(2, "msgC")
add_message(2, "msgC2")
print(f"Call 1: {select_partition(cid)} (expected 2)")
next_offset[group_id][topic][2] += 1
print(f"Call 2: {select_partition(cid)} (expected 2)")
next_offset[group_id][topic][2] += 1

print("\n--- Test 6: No messages in any assigned partition ---")
reset_test([0, 1])
print(f"Call 1: {select_partition(cid)} (expected None)")
