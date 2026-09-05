class Partition:
    def __init__(self, partition_id):
        self.partition_id = partition_id
        self.messages = []
        self.next_offset = 0

    def append(self, message):
        offset = self.next_offset

        self.messages.append({
            "offset": offset,
            "message": message
        })

        self.next_offset += 1

        return offset

partition = Partition(0)
offset = partition.append("Hello, World!")