from broker.storage import consumer_offsets, consumer_offsets_lock, next_offset, next_offset_lock,pending_acks, pending_acks_lock,resolved_offsets, resolved_offsets_lock
from pathlib import Path
import json

OFFSET_FILE = Path(__file__).parent.parent / "storage" / "offsets.json"

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
            and partition_id in consumer_offsets[group_id][topic]
        ):
            return consumer_offsets[group_id][topic][partition_id]

        return 0


def commit_offset(group_id, topic, partition_id, offset):
    with consumer_offsets_lock:

        if group_id not in consumer_offsets:
            consumer_offsets[group_id] = {}

        if topic not in consumer_offsets[group_id]:
            consumer_offsets[group_id][topic] = {}

        consumer_offsets[group_id][topic][partition_id] = offset

        save_offsets()

        print(
            f"Committed offset for group '{group_id}', "
            f"topic '{topic}', partition '{partition_id}': "
            f"{offset}"
        )


def save_offsets():
    with open(OFFSET_FILE, "w") as file:
        json.dump(consumer_offsets, file, indent=4)


def load_offsets():
    if OFFSET_FILE.exists():
        with open(OFFSET_FILE, "r") as file:
            loaded_offsets = json.load(file)

            with consumer_offsets_lock:
                consumer_offsets.clear()
                consumer_offsets.update(loaded_offsets)
    