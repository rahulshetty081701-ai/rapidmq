import socket
from common.protocol import encode_message, recv_message
from common.constants import MESSAGE_TYPE_METADATA
import json

def test():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 9092))
    
    msg = {
        "version": 1,
        "type": MESSAGE_TYPE_METADATA
    }
    s.sendall(encode_message(msg))
    
    resp = recv_message(s)
    print(json.dumps(resp, indent=2))
    s.close()

if __name__ == "__main__":
    test()
