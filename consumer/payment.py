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
        msg_content = message.get("payload", {}).get("message") if isinstance(message.get("payload"), dict) else message.get("message")
        print(
            f"Consumed message: "
            f"{msg_content}"
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