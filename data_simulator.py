from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import random
from datetime import datetime

# --- Connection Retry Logic ---
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:9092",
            api_version_auto_timeout_ms=5000,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Successfully connected to Kafka!")
    except NoBrokersAvailable:
        print("⚠️ Kafka broker not available. Retrying in 5 seconds...")
        time.sleep(5)

print("📡 Starting enhanced data simulation for Vijayawada...")
while True:
    try:
        now = datetime.now()
        data = {
            # Focus coordinates on the Vijayawada area
            "lat": random.uniform(16.45, 16.55),
            "lon": random.uniform(80.55, 80.65),
            # --- NEW FEATURES ---
            "hour_of_day": now.hour,
            "day_of_week": now.weekday(), # Monday=0, Sunday=6
            "timestamp": now.timestamp()
        }
        producer.send("geo-topic", data)
        print(f"   Sent: {data}")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred while sending data: {e}")
        time.sleep(5)
