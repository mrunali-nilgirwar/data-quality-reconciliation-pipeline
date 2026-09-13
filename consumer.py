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
received = []
for message in consumer:
    received.append(message.value)
    print("Received:", message.value)

print(f"Total messages received: {len(received)}")