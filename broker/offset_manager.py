import socket
from broker.storage import (
    consumer_offsets,
    consumer_offsets_lock,
    next_offset,
    next_offset_lock,
    pending_acks,
    pending_acks_lock,
    resolved_offsets,
    resolved_offsets_lock,
    broker_id,
)
from common.constants import (
    HOST,
    PORT,
    KNOWN_BROKERS,
    COORDINATOR_BROKER_ID,
    MESSAGE_TYPE_OFFSET_COMMIT,
)
from common.protocol import encode_message, recv_message
from pathlib import Path
import json

OFFSET_FILE = Path(__file__).parent.parent / "storage" / "offsets.json"

coordinator_offset_committer = None


def notify_coordinator_offset_commit(group_id, topic, partition_id, offset):
    """
    Notifies the coordinator (Broker 1) of a committed offset on a remote partition.
    """
    if coordinator_offset_committer is not None:
        coordinator_offset_committer(group_id, topic, partition_id, offset)
        return

    coord_addr = KNOWN_BROKERS.get(COORDINATOR_BROKER_ID, (HOST, PORT))
    msg = {
        "version": 1,
        "type": MESSAGE_TYPE_OFFSET_COMMIT,
        "group_id": group_id,
        "topic": topic,
        "partition": partition_id,
        "offset": offset,
    }
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect(coord_addr)
        s.sendall(encode_message(msg))
        recv_message(s)
        s.close()
    except OSError as e:
        print(f"Failed to sync offset to coordinator at {coord_addr}: {e}")

def advance_committed_offset(group_id, topic, partition_id):
    with pending_acks_lock:
        with resolved_offsets_lock:

            pendingoffsets = pending_acks.get(
                group_id, {}
            ).get(topic, {}).get(partition_id, set())

            resolvedoffsets = resolved_offsets.get(
                group_id, {}
            ).get(topic, {}).get(partition_id, set())

            with consumer_offsets_lock:
                committed_offset = consumer_offsets.get(
                    group_id, {}
                ).get(topic, {}).get(partition_id, 0)

            while True:

                if committed_offset in pendingoffsets:
                    pendingoffsets.remove(committed_offset)
                    committed_offset += 1

                elif committed_offset in resolvedoffsets:
                    resolvedoffsets.remove(committed_offset)
                    committed_offset += 1

                else:
                    break

    commit_offset(
        group_id,
        topic,
        partition_id,
        committed_offset
    )

    print(
        f"Advanced committed offset for group "
        f"'{group_id}', topic '{topic}', "
        f"partition '{partition_id}' "
        f"to {committed_offset}"
    )


def get_next_offset(group_id, topic, partition_id):

    if (
        group_id in next_offset
        and topic in next_offset[group_id]
        and partition_id in next_offset[group_id][topic]
    ):
        return next_offset[group_id][topic][partition_id]

    if group_id not in next_offset:
        next_offset[group_id] = {}

    if topic not in next_offset[group_id]:
        next_offset[group_id][topic] = {}

    if partition_id not in next_offset[group_id][topic]:
        next_offset[group_id][topic][partition_id] = get_offset(
            group_id,
            topic,
            partition_id
        )

    return next_offset[group_id][topic][partition_id]


def get_offset(group_id, topic, partition_id):
    with consumer_offsets_lock:
        if (
            group_id in consumer_offsets
            and topic in consumer_offsets[group_id]
        ):
            part_map = consumer_offsets[group_id][topic]
            if partition_id in part_map:
                return part_map[partition_id]
            if isinstance(partition_id, int) and str(partition_id) in part_map:
                return part_map[str(partition_id)]
            elif isinstance(partition_id, str) and partition_id.isdigit() and int(partition_id) in part_map:
                return part_map[int(partition_id)]

        return 0


def commit_offset(group_id, topic, partition_id, offset):
    try:
        partition_id = int(partition_id)
    except (ValueError, TypeError):
        pass

    with consumer_offsets_lock:
        if group_id not in consumer_offsets:
            consumer_offsets[group_id] = {}

        if topic not in consumer_offsets[group_id]:
            consumer_offsets[group_id][topic] = {}

        consumer_offsets[group_id][topic][partition_id] = offset

        is_coordinator = (broker_id["value"] == COORDINATOR_BROKER_ID)

        print(
            f"Committed offset for group '{group_id}', "
            f"topic '{topic}', partition '{partition_id}': "
            f"{offset}"
        )

    if is_coordinator:
        save_offsets()
    else:
        notify_coordinator_offset_commit(group_id, topic, partition_id, offset)


def save_offsets():
    # Only coordinator writes to the shared offset file
    if broker_id["value"] != COORDINATOR_BROKER_ID:
        return

    with consumer_offsets_lock:
        to_dump = {}
        for grp, grp_topics in consumer_offsets.items():
            to_dump[grp] = {}
            for top, parts in grp_topics.items():
                to_dump[grp][top] = {str(p): off for p, off in parts.items()}

    with open(OFFSET_FILE, "w") as file:
        json.dump(to_dump, file, indent=4)


def load_offsets():
    if OFFSET_FILE.exists():
        with open(OFFSET_FILE, "r") as file:
            loaded_offsets = json.load(file)
            
            converted_offsets = {}
            for group, topics in loaded_offsets.items():
                converted_offsets[group] = {}
                for topic, partitions in topics.items():
                    converted_offsets[group][topic] = {
                        int(p_id): offset for p_id, offset in partitions.items()
                    }

            with consumer_offsets_lock:
                consumer_offsets.clear()
                consumer_offsets.update(converted_offsets)
    