import os
import json

from .storage import message_id
from .partition import Partition

STORAGE_DIR = "storage"


def append_message(topic, partition_id, offset, message):
    topic_dir = os.path.join(STORAGE_DIR, topic)
    os.makedirs(topic_dir, exist_ok=True)

    file_path = os.path.join(
        topic_dir,
        f"partition-{partition_id}.log"
    )

    stored_message = {
        "partition": partition_id,
        "offset": offset,
        **message
    }

    with open(file_path, "a") as file:
        file.write(json.dumps(stored_message))
        file.write("\n")


def load_messages():
    if not os.path.exists(STORAGE_DIR):
        return {}

    loaded_topics = {}
    highest_id = 0

    for topic in os.listdir(STORAGE_DIR):

        topic_dir = os.path.join(STORAGE_DIR, topic)

        if not os.path.isdir(topic_dir):
            continue

        loaded_topics[topic] = {
            0: Partition(0),
            1: Partition(1),
            2: Partition(2)
        }

        for file_name in os.listdir(topic_dir):

            if not file_name.startswith("partition-"):
                continue

            if not file_name.endswith(".log"):
                continue

            file_path = os.path.join(topic_dir, file_name)

            with open(file_path, "r") as file:

                for line in file:
                    line = line.strip()

                    if not line:
                        continue

                    message = json.loads(line)

                    if (
                        isinstance(message.get("message"), dict)
                        and "message_id" in message["message"]
                    ):
                        inner = message.pop("message")
                        message.update(inner)

                    message_id_value = message.get("message_id", 0)
                    highest_id = max(
                        highest_id,
                        message_id_value
                    )

                    partition_id = message.get("partition", 0)

                    if partition_id not in loaded_topics[topic]:
                        loaded_topics[topic][partition_id] = Partition(
                            partition_id
                        )

                    loaded_topics[topic][partition_id].append(message)

    message_id["value"] = highest_id

    return loaded_topics