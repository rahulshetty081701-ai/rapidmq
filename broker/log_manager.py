import os
import json

from .storage import message_id
from .partition import Partition

STORAGE_DIR = "storage"

def append_message(topic, partition_id, offset, message):
    file_path = f"{STORAGE_DIR}/{topic}.log"

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

    for file_name in os.listdir(STORAGE_DIR):

        if not file_name.endswith(".log"):
            continue

        topic = file_name.replace(".log", "")

        loaded_topics[topic] = {
            0: Partition(0),
            1: Partition(1),
            2: Partition(2)
        }

        file_path = f"{STORAGE_DIR}/{file_name}"

        with open(file_path, "r") as file:

            for line in file:
                line = line.strip()
                if not line:
                    continue
                message = json.loads(line)

                if isinstance(message.get("message"), dict) and "message_id" in message["message"]:
                    inner = message.pop("message")
                    message.update(inner)

                message_id_value = message.get("message_id", 0)
                highest_id = max(highest_id, message_id_value)

                partition_id = message.get("partition", 0)

                loaded_topics[topic][partition_id].append(message)

    message_id["value"] = highest_id
    return loaded_topics