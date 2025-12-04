import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

print("⏳ Loading history.csv...")

# 1. Load the historical dataset
try:
    df = pd.read_csv("history.csv")
    print(f"   Loaded {len(df)} records.")
except FileNotFoundError:
    print("❌ Error: history.csv not found. Please run process_data.py first!")
    exit()

# 2. Prepare Features and Target
# The model will now learn the complex relationship between Time, Location, Traffic, and Surge
X = df[['lat', 'lon', 'hour_of_day', 'day_of_week', 'traffic_congestion']]
y = df['surge_multiplier']

# 3. Train the Model
print("🧠 Training Random Forest Regressor...")
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X, y)

# 4. Save the Model
joblib.dump(model, "geo_model.pkl")
print("✅ Model trained on historical data and saved as 'geo_model.pkl'")