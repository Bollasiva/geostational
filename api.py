from flask import Flask, jsonify
from influxdb_client import InfluxDBClient
import time

app = Flask(__name__)

bucket = "geo_db"
org = "myorg"
token = "RELIABLE_ADMIN_TOKEN_123"
url = "http://localhost:8086"

# --- Connection Retry Logic ---
# This loop ensures the API server doesn't start until it can connect to the database.
client = None
while client is None:
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        if client.ping():
            print("✅ API successfully connected to InfluxDB!")
            break
    except Exception as e:
        print(f"⚠️ API could not connect to InfluxDB. Retrying in 5 seconds... Error: {e}")
        time.sleep(5)

query_api = client.query_api()

# This is the endpoint you access in your browser.
@app.route("/surge-predictions", methods=["GET"])
def get_predictions():
    try:
        # --- UPDATED QUERY ---
        # This Flux query is designed to fetch the latest surge prediction data.
        # pivot() reshapes the data from InfluxDB's format into a more user-friendly
        # JSON object for each prediction.
        query = f'''
            from(bucket: "{bucket}")
              |> range(start: -1h)
              |> filter(fn: (r) => r._measurement == "surge_predictions")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: 10)
        '''
        tables = query_api.query(query, org=org)

        results = []
        for table in tables:
            for record in table.records:
                # The pivot() function makes the data much easier to work with here.
                results.append(record.values)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # This makes the Flask server accessible from outside the Docker container.
    app.run(host="0.0.0.0", port=5000)