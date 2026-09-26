from broker.storage import consumers, consumers_lock, topics, queue_lock, next_offset_lock
from broker.offset_manager import get_next_offset
import threading

# Scheduler state
next_partition_index = {}
scheduler_lock = threading.Lock()

def select_partition(consumer_id):
    """
    Select an assigned partition that has available messages for the given consumer.
    Uses round-robin based on the consumer's 'next_partition_index' state.
    """
    with consumers_lock:
        consumer = consumers.get(consumer_id)
        if not consumer or consumer.get("status") != "ACTIVE":
            return None
        
        group_id = consumer["group_id"]
        topic = consumer["topic"]
        partitions = consumer.get("partitions", [])

    if not partitions:
        return None

    num_partitions = len(partitions)
    
    with scheduler_lock:
        next_idx = next_partition_index.get(consumer_id, 0)
        # Ensure next_idx is within bounds in case partitions shrank after a rebalance
        if next_idx >= num_partitions:
            next_idx = 0
            
    # Check all assigned partitions, starting from next_idx
    for i in range(num_partitions):
        idx_to_check = (next_idx + i) % num_partitions
        partition_id = partitions[idx_to_check]
        
        has_message = False
        with queue_lock:
            if topic in topics and partition_id in topics[topic]:
                partition = topics[topic][partition_id]
                
                with next_offset_lock:
                    offset = get_next_offset(group_id, topic, partition_id)
                    if offset < len(partition.messages):
                        has_message = True
                        
        if has_message:
            # Advance index for the next call to ensure fair round-robin scheduling
            with scheduler_lock:
                next_partition_index[consumer_id] = (idx_to_check + 1) % num_partitions
            return partition_id
            
    return None
