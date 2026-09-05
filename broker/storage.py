import threading
topics ={}
queue_lock = threading.Lock()

message_id = {"value": 0}
id_lock = threading.Lock()


in_flight = {}
in_flight_lock = threading.Lock()
print("Storage initialized:in_flight",in_flight)

consumer_offsets = {}
print("Storage initialized:consumer_offsets",consumer_offsets)
consumer_offsets_lock = threading.Lock()

next_offset = {}
next_offset_lock = threading.Lock()

pending_acks = {}
pending_acks_lock = threading.Lock()

retry_queue = {}
retry_queue_lock = threading.Lock()

dead_letter_queue = {}
dead_letter_queue_lock = threading.Lock()

resolved_offsets = {}
resolved_offsets_lock = threading.Lock()

consumers = {}
consumers_lock = threading.Lock()