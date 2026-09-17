class Partition:
    def __init__(self, partition_id):
        self.partition_id = partition_id
        self.messages = []
        self.next_offset = 0

    def append(self, message):
        offset = self.next_offset

        message["offset"] = offset
        message["partition"] = self.partition_id

        self.messages.append(message)

        self.next_offset += 1

        return offset