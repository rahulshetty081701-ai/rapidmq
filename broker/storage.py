import threading
topics ={}
queue_lock = threading.Lock()

message_id = {"value": 0}
id_lock = threading.Lock()


in_flight = {}
in_flight_lock = threading.Lock()