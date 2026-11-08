from client.producer import Producer

p = Producer()
p.connect()
p.fetch_metadata()

# Force an invalid mapping
print("\n--- Tampering with metadata to simulate staleness ---")
p.partition_owners["orders"]["1"] = 3 # Map partition 1 to Broker 3 (incorrect, belongs to 2)

# Publish to partition 1
print("\n--- Publishing to force ownership error ---")
p.topic_partition_state["orders"] = 1 # Force it to select index 1 (which is partition 1)
p.publish("orders", {"message": "Test refresh"})

print("\n--- Metadata after refresh ---")
print(p.partition_owners["orders"])
p.close()
