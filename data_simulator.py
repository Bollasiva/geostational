from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    data = {
        "lat": random.uniform(-90, 90),
        "lon": random.uniform(-180, 180),
        "timestamp": time.time()
    }
    producer.send("geo-topic", data)
    print("📡 Sent:", data)
    time.sleep(2)
