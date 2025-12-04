import pandas as pd
import numpy as np
from geopy.distance import great_circle

print("⏳ Loading NYC Taxi Data...")
# Load data
df = pd.read_csv("train.csv", nrows=10000) 

# --- TELEPORTATION CONFIGURATION ---
NYC_CENTER_LAT = 40.7128
NYC_CENTER_LON = -74.0060
VJA_CENTER_LAT = 16.5062
VJA_CENTER_LON = 80.6480

lat_offset = VJA_CENTER_LAT - NYC_CENTER_LAT
lon_offset = VJA_CENTER_LON - NYC_CENTER_LON

print("🛠️ Processing features and calculating distances...")

# 1. Shift Coordinates
df['lat'] = df['pickup_latitude'] + lat_offset
df['lon'] = df['pickup_longitude'] + lon_offset

# 2. Process Time
df['datetime'] = pd.to_datetime(df['pickup_datetime'])
df['hour_of_day'] = df['datetime'].dt.hour
df['day_of_week'] = df['datetime'].dt.dayofweek

# 3. CALCULATE DISTANCE (New Step!)
def get_distance(row):
    start = (row['pickup_latitude'], row['pickup_longitude'])
    end = (row['dropoff_latitude'], row['dropoff_longitude'])
    try:
        return great_circle(start, end).kilometers
    except:
        return 0.0

df['distance_km'] = df.apply(get_distance, axis=1)

# 4. CALCULATE CONGESTION
def calculate_congestion(row):
    try:
        dist = row['distance_km']
        duration_h = row['trip_duration'] / 3600
        if duration_h <= 0 or dist <= 0: return 0.0
        speed_kmh = dist / duration_h
        free_flow = 40.0
        congestion = 1 - (speed_kmh / free_flow)
        return max(0.0, min(1.0, congestion))
    except:
        return 0.0

df['traffic_congestion'] = df.apply(calculate_congestion, axis=1)

# 5. CALCULATE SURGE
hourly_demand = df.groupby(['day_of_week', 'hour_of_day']).size().reset_index(name='trip_count')
df = df.merge(hourly_demand, on=['day_of_week', 'hour_of_day'])
max_trips = df['trip_count'].max()
min_trips = df['trip_count'].min()

def calculate_surge(trip_count):
    intensity = (trip_count - min_trips) / (max_trips - min_trips)
    return 1.0 + (intensity * 2.0)

df['surge_multiplier'] = df['trip_count'].apply(calculate_surge)

# 6. SAVE WITH DISTANCE
# We added 'distance_km' to this list
final_df = df[['lat', 'lon', 'hour_of_day', 'day_of_week', 'traffic_congestion', 'surge_multiplier', 'distance_km']]

final_df.to_csv("history.csv", index=False)
print("✅ Success! history.csv now includes Trip Distance.")
print(final_df.head())