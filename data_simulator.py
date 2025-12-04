import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import requests
import random  # <--- Make sure this is imported
from datetime import datetime

# --- CONFIGURATION ---
TOMTOM_API_KEY = "67Wy3uhYUAQ5W0YBAj1pRDmoi8joyp5z"
DATA_FILE = "history.csv"

# Function to get the street name (Reverse Geocoding)
def get_location_name(lat, lon):
    try:
        url = f"https://api.tomtom.com/search/2/reverseGeocode/{lat},{lon}.json?key={TOMTOM_API_KEY}&radius=100"
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            if 'addresses' in data and len(data['addresses']) > 0:
                addr = data['addresses'][0]['address']
                return addr.get('streetName', addr.get('municipality', 'Vijayawada'))
    except Exception:
        pass
    return "Vijayawada Location"

# --- KAFKA CONNECTION ---
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

print(f"📡 Replaying Historical Data (with Festival Simulation)...")

try:
    df = pd.read_csv(DATA_FILE)

    while True:
        for index, row in df.iterrows():
            
            # 1. Get Real Location Name
            location_name = get_location_name(row['lat'], row['lon'])

            # 2. SIMULATE FESTIVAL (20% Chance)
            is_festival = random.random() < 0.20
            event_type = "Festival" if is_festival else "Regular"
            icon = "🎉" if is_festival else "📅"

            # 3. Construct Complete Message
            message = {
                "lat": row['lat'],
                "lon": row['lon'],
                "location_name": location_name,
                "hour_of_day": int(row['hour_of_day']),
                "day_of_week": int(row['day_of_week']),
                "traffic_congestion": row['traffic_congestion'],
                "distance_km": row['distance_km'],
                "event_type": event_type,   # <--- RESTORED THIS
                "timestamp": datetime.now().timestamp()
            }

            producer.send("geo-topic", message)

            print(f"📍 {location_name}")
            print(f"   {icon} Mode: {event_type} | 📏 Dist: {row['distance_km']:.1f} km")
            print(f"   🚕 Sent Trip #{index+1}")
            print("-" * 50)

            time.sleep(2)

        print("🔄 Reached end of history.csv. Restarting loop...")

except FileNotFoundError:
    print("❌ Error: history.csv not found.")