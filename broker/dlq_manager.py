from pathlib import Path
import json

DLQ_DIR = Path(__file__).parent.parent / "storage" / "dlq"

def append_to_dlq(group_id, topic, data):
    DLQ_DIR.mkdir(parents=True, exist_ok=True)

    dlq_file = DLQ_DIR / f"{group_id}_{topic}.log"

    with open(dlq_file, "a") as file:
        file.write(json.dumps(data) + "\n")


def load_dlq():
    loaded_dlq = {}

    if not DLQ_DIR.exists():
        return loaded_dlq

    for dlq_file in DLQ_DIR.glob("*.log"):
        filename = dlq_file.stem

        # group_topic
        group_id, topic = filename.split("_", 1)

        loaded_dlq.setdefault(group_id, {})
        loaded_dlq[group_id].setdefault(topic, [])

        with open(dlq_file, "r") as file:
            for line in file:
                line = line.strip()

                if line:
                    loaded_dlq[group_id][topic].append(
                        json.loads(line)
                    )

    return loaded_dlq