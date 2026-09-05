import json
import struct
MAX_MESSAGE_SIZE = 1_000_000

def encode_message(message: dict) -> bytes:
    """Convert a Python dictionary into a length-prefixed message."""

    data = json.dumps(message).encode("utf-8")
    length = len(data)

    return struct.pack("!I", length) + data


def decode_message(data: bytes) -> dict:
    """Convert bytes into a Python dictionary."""

    return json.loads(data.decode("utf-8"))


def recv_message(client_socket):
    """Receive exactly one length-prefixed message from a socket."""

    header = client_socket.recv(4)
    print(f"Received header: {header}")
    if not header:
        print("Client disconnected while waiting for message header.")
        return None

    length = struct.unpack("!I", header)[0]

    if length > MAX_MESSAGE_SIZE:
        raise ValueError("Message too large")

    data = b""

    while len(data) < length:
        chunk = client_socket.recv(length - len(data))

        if not chunk:
            return None

        data += chunk

    return decode_message(data)
