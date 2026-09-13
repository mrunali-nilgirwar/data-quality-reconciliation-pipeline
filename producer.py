import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open("source.json") as f:
    source_data = json.load(f)

for row in source_data:
    producer.send("orders-topic", value=row)
    print("Sent:", row)

producer.flush()
print("All messages sent.")