import time

from consumer.consumer_thread import ConsumerClient


GROUP_ID = "payment"
TOPIC = "order"


consumer = ConsumerClient(
    group_id=GROUP_ID,
    topic=TOPIC
)

try:

    consumer.register()

    while True:

        message = consumer.consume()
        print("message received from broker: ", message)
        if message["type"] == "EMPTY":
            print("No messages available to consume.")
            time.sleep(1)
            continue

        print(
            f"Consumed message: "
            f"{message['payload']['message']}"
        )

        # Simulate processing
        time.sleep(1)

        consumer.ack(message)

except KeyboardInterrupt:

    print("Consumer interrupted by user.")

except Exception as e:

    print(f"Error: {e}")

finally:

    consumer.close()