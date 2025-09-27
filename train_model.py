import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib

print("Starting enhanced model training...")

# Create more realistic dummy data for Vijayawada surge pricing.
# We'll teach the model that demand is high during morning/evening rush hours on weekdays.
data = {
    'lat': [16.50, 16.51, 16.52, 16.45, 16.55, 16.51, 16.48],
    'lon': [80.61, 80.62, 80.60, 80.65, 80.55, 80.62, 80.58],
    'hour_of_day': [9, 19, 14, 8, 3, 20, 13], # Morning rush, Evening rush, Midday, Weekend etc.
    'day_of_week': [4, 4, 1, 5, 6, 0, 5], # Friday, Friday, Tuesday, Saturday, Sunday, Monday, Saturday
    'surge': [1.8, 2.0, 1.1, 1.4, 1.0, 1.6, 1.2] # The target value we want to predict
}
df = pd.DataFrame(data)

# Define the features (X) and the target (y)
X = df[['lat', 'lon', 'hour_of_day', 'day_of_week']]
y = df['surge']

# --- UPGRADED MODEL ---
# Use a RandomForestRegressor, which is much better at learning complex patterns.
# 
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X, y)

# Save the new, smarter model to a file
joblib.dump(model, "geo_model.pkl")

print("✅ Enhanced RandomForest model trained and saved as geo_model.pkl")