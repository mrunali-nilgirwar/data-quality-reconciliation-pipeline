import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=5000
)

print("Listening for messages...")
consumer_data = []

for message in consumer:
    consumer_data.append(message.value)
    print("Received:", message.value)

with open("consumed_orders.json", "w") as f:
    json.dump(consumer_data, f, indent=2)

print(f"Total messages received: {len(consumer_data)}")
print("Saved to consumed_orders.json")