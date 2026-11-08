from broker.storage import (
    consumers,
    consumers_lock,
    partition_assignments,
    partition_assignments_lock,
    group_generations,
    group_generations_lock
)
from broker.metadata import get_topic_partitions

def rebalance(group_id, topic):
    """
    Rebalance partitions for a given consumer group and topic.
    Sorts active consumers and partitions, then performs a round-robin assignment.
    Increments group assignment generation if the partition mapping has changed.
    """
    # 1 & 2. Get all configured partitions for the specified topic across the cluster
    partition_ids = sorted(list(get_topic_partitions(topic)))
        
    if not partition_ids:
        return {}
        
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
            
    # 5 & 6. Update partition_assignments, group_generations, and consumers atomically
    with group_generations_lock:
        with partition_assignments_lock:
            with consumers_lock:
                if group_id not in partition_assignments:
                    partition_assignments[group_id] = {}
                
                prev_mapping = dict(partition_assignments.get(group_id, {}).get(topic, {}))
                new_mapping = {}
                for cid, assigned_parts in new_assignment.items():
                    for p_id in assigned_parts:
                        new_mapping[p_id] = cid

                group_key = (group_id, topic)
                if group_key not in group_generations:
                    group_generations[group_key] = 1
                elif new_mapping != prev_mapping:
                    group_generations[group_key] += 1

                current_gen = group_generations[group_key]

                # Completely overwrite the topic assignments
                partition_assignments[group_id][topic] = dict(new_mapping)
                        
                # Update partitions and generation for all consumers of this group/topic
                for cid, cinfo in consumers.items():
                    if cinfo.get("group_id") == group_id and cinfo.get("topic") == topic:
                        if cid in new_assignment:
                            cinfo["partitions"] = list(new_assignment[cid])
                            cinfo["generation"] = current_gen
                        else:
                            cinfo["partitions"] = []
                            cinfo["generation"] = current_gen
                            
    return new_assignment
