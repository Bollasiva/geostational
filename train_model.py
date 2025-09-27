import pandas as pd
from sklearn.linear_model import LinearRegression
import joblib

# Dummy training data
data = pd.DataFrame({
    'lat': [10, 20, 30, 40, 50],
    'lon': [15, 25, 35, 45, 55],
    'value': [100, 200, 300, 400, 500]
})

X = data[['lat', 'lon']]
y = data['value']

model = LinearRegression()
model.fit(X, y)

# Save model
joblib.dump(model, "geo_model.pkl")
print("✅ Model trained and saved as geo_model.pkl")
