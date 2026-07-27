import json


def encode_message(message: dict) -> bytes:
    """Convert a Python dictionary into bytes."""
    return json.dumps(message).encode("utf-8")


def decode_message(data: bytes) -> dict:
    """Convert bytes into a Python dictionary."""
    return json.loads(data.decode("utf-8"))