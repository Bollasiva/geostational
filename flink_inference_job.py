import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError
import time

# --- Load the Enhanced ML model ---
# This loads the geo_model.pkl file created by your train_model.py script.
model = joblib.load("geo_model.pkl")

# --- Kafka Connection Logic ---
# This loop will keep trying to connect until the Kafka service in Docker is ready.
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            "geo-topic",
            bootstrap_servers="127.0.0.1:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        print("✅ Successfully connected to Kafka!")
    except NoBrokersAvailable:
        print("⚠️ Kafka broker not available. Retrying in 5 seconds...")
        time.sleep(5)

# --- InfluxDB Client Setup ---
# It uses the same credentials as your docker-compose.yml file.
bucket = "geo_db"
org = "myorg"
token = "RELIABLE_ADMIN_TOKEN_123"
url = "http://localhost:8086"
client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

print("✅ Flink job started (Kafka → InfluxDB 2.x)")
print("👂 Listening for messages...")

# This loop runs forever, processing messages as they arrive in the Kafka topic.
for message in consumer:
    data = message.value

    # --- USE THE NEW FEATURES ---
    # It extracts all the necessary features from the incoming message.
    features = {
        'lat': data["lat"],
        'lon': data["lon"],
        'hour_of_day': data["hour_of_day"],
        'day_of_week': data["day_of_week"]
    }

    # Create a pandas DataFrame with feature names in the correct order for the model.
    # This ensures the model receives data in the format it expects.
    input_df = pd.DataFrame([features], columns=['lat', 'lon', 'hour_of_day', 'day_of_week'])

    # Make prediction using the enhanced RandomForest model.
    prediction = model.predict(input_df)[0]

    # --- STORE THE ENHANCED DATA ---
    # A "point" is a single record in InfluxDB. We now store all the relevant data.
    point = Point("surge_predictions") \
        .tag("day_of_week", features['day_of_week']) \
        .field("lat", features['lat']) \
        .field("lon", features['lon']) \
        .field("hour", features['hour_of_day']) \
        .field("surge_multiplier", float(prediction))

    try:
        write_api.write(bucket=bucket, org=org, record=point)
        print(f"   📝 Stored: lat={features['lat']:.2f}, lon={features['lon']:.2f}, hour={features['hour_of_day']}, surge={prediction:.2f}")
    except InfluxDBError as e:
        print(f"❌ Error writing to InfluxDB: {e}")