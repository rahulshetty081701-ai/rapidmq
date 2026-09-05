import json
import os
from .storage import message_id

STORAGE_DIR = "storage"


if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)


def append_message(topic, message):

    file_path = f"{STORAGE_DIR}/{topic}.log"

    with open(file_path, "a") as file:
        file.write(json.dumps(message))
        file.write("\n")

import os
import json


def load_messages():

    if not os.path.exists(STORAGE_DIR):
        return {}

    loaded_topics = {}
    highest_id = 0

    for file_name in os.listdir(STORAGE_DIR):

        if file_name.endswith(".log"):

            topic = file_name.replace(".log", "")

            loaded_topics[topic] = []

            file_path = f"{STORAGE_DIR}/{file_name}"

            with open(file_path, "r") as file:

                for line in file:
                    message = json.loads(line)
                    loaded_topics[topic].append(message)
                    highest_id = max(highest_id, message.get("message_id", 0))
    message_id["value"] = highest_id
    return loaded_topics