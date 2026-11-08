from common.constants import KNOWN_BROKERS, DEFAULT_PARTITION_COUNT

def get_topic_partitions(topic: str) -> list[int]:
    """
    Returns the cluster-wide configured partition IDs for the given topic.
    Currently defaults to partition IDs [0 .. DEFAULT_PARTITION_COUNT - 1].
    """
    return list(range(DEFAULT_PARTITION_COUNT))

def get_partition_owner(topic: str, partition: int) -> int:
    """
    Returns the broker_id that owns the specified (topic, partition).
    Uses a simple deterministic round-robin assignment based on the partition ID.
    """
    broker_ids = sorted(list(KNOWN_BROKERS.keys()))
    if not broker_ids:
        return 1
        
    return broker_ids[partition % len(broker_ids)]
