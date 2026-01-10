from google.cloud import pubsub_v1
import json

PROJECT_ID = "vctbatch-dev45"
SUBSCRIPTION_ID = "employee-pull-sub"  # PULL subscription

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = message.data.decode("utf-8")
        payload = json.loads(data)

        print("Received message:", payload)
        print("Attributes:", message.attributes)

        # ✅ Acknowledge after successful processing
        #message.ack()

    except Exception as e:
        print("Processing failed:", e)
        # ❌ Do not ack → message will be redelivered
        message.nack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback
)

print(f"Listening on {subscription_path}...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    subscriber.close()
