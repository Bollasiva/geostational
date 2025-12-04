# ---------- CONFIGURATION ----------
CURRENT_PETROL_PRICE = 109.50
CURRENT_DIESEL_PRICE = 98.30

def format_wait_time(minutes):
    """Converts minutes into readable string."""
    if minutes < 60:
        return f"{minutes} mins"
    hrs = minutes // 60
    mins = minutes % 60
    if mins == 0:
        return f"{hrs} hr"
    return f"{hrs} hr {mins} mins"


def get_future_traffic_trend(hour, current_traffic):
    """
    Dummy function — replace with your real traffic model.
    """
    if 8 <= hour <= 10 or 17 <= hour <= 20:
        return min(1.0, current_traffic + 0.1)
    return max(0.0, current_traffic - 0.1)


def get_dynamic_rates(vehicle_type, hour, location_name):
    """
    Dynamic Real-World Pricing Engine (Fuel + Mileage + Night + Rural)
    """
    if vehicle_type == "Rapido (Bike)":
        mileage = 45
        fuel_cost = CURRENT_PETROL_PRICE / mileage
        maintenance = 1.50
        margin = 2.00
        base_rate = 15.0

    elif vehicle_type == "Ola (Auto)":
        mileage = 25
        fuel_cost = 3.50
        maintenance = 2.00
        margin = 3.00
        base_rate = 30.0

    elif vehicle_type == "Uber (Car)":
        mileage = 15
        fuel_cost = CURRENT_DIESEL_PRICE / mileage
        maintenance = 4.00
        margin = 5.00
        base_rate = 45.0

    per_km_rate = fuel_cost + maintenance + margin

    if hour >= 23 or hour <= 5:
        base_rate *= 1.3
        per_km_rate *= 1.1

    if "Rural" in location_name or "Outskirts" in location_name:
        base_rate += 20.0

    return {
        "base": round(base_rate, 2),
        "per_km": round(per_km_rate, 2),
        "min": base_rate * 1.5,
        "surge_sens": 1.0
    }

# ---------- MAIN ENGINE ----------
import json
import joblib
import pandas as pd
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WriteOptions

model = joblib.load("geo_model.pkl")

consumer = KafkaConsumer(
    "geo-topic",
    bootstrap_servers="127.0.0.1:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

client = InfluxDBClient(url="http://localhost:8086",
                        token="RELIABLE_ADMIN_TOKEN_123",
                        org="myorg")
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

print("🚀 Dynamic AI Pricing Engine Started")

FESTIVAL_MULTIPLIER = 1.4


# ---------- KAFKA LOOP ----------
for message in consumer:
    data = message.value

    features = {
        "lat": data["lat"], "lon": data["lon"],
        "hour_of_day": data["hour_of_day"],
        "day_of_week": data["day_of_week"],
        "traffic_congestion": data.get("traffic_congestion", 0.0),
        "location_name": data.get("location_name", "Unknown"),
        "distance_km": data.get("distance_km", 5.0),
        "event_type": data.get("event_type", "Regular")
    }

    # -------- SURGE NOW --------
    input_now = pd.DataFrame([features],
        columns=['lat','lon','hour_of_day','day_of_week','traffic_congestion'])

    surge_now = float(model.predict(input_now)[0])

    # -------- PRICE NOW --------
    prices_now = {}
    is_festival = (features['event_type'] == "Festival")
    season_multiplier = FESTIVAL_MULTIPLIER if is_festival else 1.0

    for vehicle in ["Rapido (Bike)", "Ola (Auto)", "Uber (Car)"]:

        rates = get_dynamic_rates(vehicle, features['hour_of_day'], features['location_name'])

        if "Bike" in vehicle: rates['surge_sens'] = 1.2
        elif "Auto" in vehicle: rates['surge_sens'] = 0.9
        else: rates['surge_sens'] = 1.0

        veh_surge = 1.0 + ((surge_now - 1.0) * rates['surge_sens'])
        total_mult = veh_surge * season_multiplier

        cost_now = max(
            rates['min'],
            (rates['base'] + features['distance_km'] * rates['per_km']) * total_mult
        )
        prices_now[vehicle] = cost_now

    # ---------- MINUTE-LEVEL FUTURE SCHEDULER ----------
    best_savings = 0
    best_wait_str = ""
    recommendation = "Book Now!"

    check_intervals = [15, 30, 45, 60, 90]

    for wait_mins in check_intervals:
        wait_fraction = wait_mins / 60.0
        check_hour = (features["hour_of_day"] + wait_fraction) % 24

        future_traffic = get_future_traffic_trend(int(check_hour), features["traffic_congestion"])

        input_future = pd.DataFrame([{
            **features,
            "hour_of_day": check_hour,
            "traffic_congestion": future_traffic
        }], columns=['lat','lon','hour_of_day','day_of_week','traffic_congestion'])

        pred_surge = float(model.predict(input_future)[0])

        # Future Uber Car price
        uber_rates = get_dynamic_rates("Uber (Car)", check_hour, features["location_name"])
        uber_rates["surge_sens"] = 1.0

        veh_surge = 1.0 + ((pred_surge - 1.0) * uber_rates['surge_sens'])
        total_mult = veh_surge * season_multiplier

        future_cost = max(
            uber_rates['min'],
            (uber_rates['base'] + features['distance_km'] * uber_rates['per_km']) * total_mult
        )

        current_uber_price = prices_now["Uber (Car)"]
        savings = current_uber_price - future_cost

        if savings > best_savings:
            best_savings = savings
            best_wait_str = format_wait_time(wait_mins)

    # ---------- FINAL ADVICE ----------
    if best_savings > 20:
        recommendation = f"WAIT! Save ₹{int(best_savings)} in {best_wait_str}"

    # ---------- ECO TRACKING ----------
    co2_bike = features['distance_km'] * 0.03
    co2_car = features['distance_km'] * 0.14
    co2_savings = co2_car - co2_bike

    # ---------- DATABASE SAVE ----------
    point = Point("smart_analytics") \
        .tag("location", features['location_name']) \
        .tag("event_type", features['event_type']) \
        .field("car_price", prices_now["Uber (Car)"]) \
        .field("bike_price", prices_now["Rapido (Bike)"]) \
        .field("future_savings", float(best_savings)) \
        .field("co2_grams_car", co2_car * 1000) \
        .field("co2_saved_by_bike", co2_savings * 1000)

    write_api.write(bucket="geo_db", org="myorg", record=point)

    # ---------- CONSOLE OUTPUT ----------
    print(f"📍 {features['location_name']} | {features['event_type']}")
    print(f"   📏 Dist: {features['distance_km']} km")
    print(f"   🚗 Car: ₹{prices_now['Uber (Car)']:.0f} (💨 {co2_car:.2f} kg CO₂)")
    print(f"   🏍️  Bike: ₹{prices_now['Rapido (Bike)']:.0f} (🍃 Saves {co2_savings:.2f} kg CO₂)")
    print(f"   🔮 AI Advice: \033[93m{recommendation}\033[0m")
    print("-" * 50)
