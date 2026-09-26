from broker.storage import (
    consumers,
    consumers_lock,
    partition_assignments,
    partition_assignments_lock,
    topics,
    queue_lock
)

def rebalance(group_id, topic):
    """
    Rebalance partitions for a given consumer group and topic.
    Sorts active consumers and partitions, then performs a round-robin assignment.
    """
    # 1 & 2. Get all partitions for the specified topic
    with queue_lock:
        if topic not in topics:
            return
        partition_ids = sorted(list(topics[topic].keys()))
        
    if not partition_ids:
        return
        
    # 3. Find all ACTIVE consumers belonging to the specified group_id and topic
    with consumers_lock:
        active_consumers = [
            cid for cid, cinfo in consumers.items()
            if cinfo.get("group_id") == group_id 
            and cinfo.get("topic") == topic 
            and cinfo.get("status") == "ACTIVE"
        ]
        
    # Sort consumers to ensure deterministic assignment
    active_consumers.sort()
    
    # 4. Calculate a balanced assignment using round-robin
    new_assignment = {cid: [] for cid in active_consumers}
    if active_consumers:
        num_consumers = len(active_consumers)
        for i, p_id in enumerate(partition_ids):
            assigned_cid = active_consumers[i % num_consumers]
            new_assignment[assigned_cid].append(p_id)
            
    # 5 & 6. Update partition_assignments and consumers atomically
    # We acquire partition_assignments_lock first, then consumers_lock 
    # to match the lock ordering in handle_register and prevent deadlocks.
    with partition_assignments_lock:
        with consumers_lock:
            if group_id not in partition_assignments:
                partition_assignments[group_id] = {}
            
            # Completely overwrite the topic assignments
            partition_assignments[group_id][topic] = {}
            for cid, assigned_parts in new_assignment.items():
                for p_id in assigned_parts:
                    partition_assignments[group_id][topic][p_id] = cid
                    
            # Clear old partitions for all consumers of this group/topic to avoid stale entries
            # and use list() to avoid shared references.
            for cid, cinfo in consumers.items():
                if cinfo.get("group_id") == group_id and cinfo.get("topic") == topic:
                    if cid in new_assignment:
                        cinfo["partitions"] = list(new_assignment[cid])
                    else:
                        cinfo["partitions"] = []
                        
    return new_assignment
