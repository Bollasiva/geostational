from flask import Flask, jsonify
from influxdb_client import InfluxDBClient

app = Flask(__name__)

bucket = "geo_db"
org = "myorg"
token = "mytoken"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

@app.route("/predictions", methods=["GET"])
def get_predictions():
    query = f'from(bucket: "{bucket}") |> range(start: -1h) |> sort(columns: ["_time"], desc: true) |> limit(n: 10)'
    tables = query_api.query(query, org=org)

    results = []
    for table in tables:
        for record in table.records:
            results.append({
                "time": str(record["_time"]),
                "field": record["_field"],
                "value": record["_value"]
            })
    return jsonify(results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
