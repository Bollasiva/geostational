import json
import joblib
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Load ML model
model = joblib.load("geo_model.pkl")

# Kafka consumer
consumer = KafkaConsumer(
    "geo-topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# InfluxDB v2 client
bucket = "geo_db"
org = "myorg"
token = "mytoken"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

print("✅ Flink job started (Kafka → InfluxDB 2.x)")

for message in consumer:
    data = message.value
    lat, lon = data["lat"], data["lon"]
    prediction = model.predict([[lat, lon]])[0]

    point = Point("geo_predictions") \
        .field("lat", lat) \
        .field("lon", lon) \
        .field("prediction", float(prediction))

    write_api.write(bucket=bucket, org=org, record=point)

    print(f"📝 Stored: lat={lat}, lon={lon}, prediction={prediction}")
