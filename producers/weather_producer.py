import time
import json
import requests
from kafka import KafkaProducer

# --- CONFIGURATION ---
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'weather_forecast'
# List of cities to monitor
CITIES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "London": {"lat": 51.5074, "lon": -0.1278},
    "Madrid": {"lat": 40.4168, "lon": -3.7038},
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "New York": {"lat": 40.7128, "lon": -74.0060}
}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_weather_data(city_name, lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    try:
        # Timeout set to 5 seconds to prevent the script from hanging
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return {
            "city": city_name,
            "temperature": data['current_weather']['temperature'],
            "windspeed": data['current_weather']['windspeed'],
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "mode": "live"
        }
    except Exception as e:
        # FALLBACK: If API fails, send simulated data so Spark Join doesn't break
        print(f"⚠️ API Error for {city_name}: {e}. Switching to Simulated Data.")
        return {
            "city": city_name,
            "temperature": 15.0, # Default safe value
            "windspeed": 5.5,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "mode": "simulated"
        }

print("🚀 Resilient Weather Producer is LIVE...")

try:
    while True:
        for city, coords in CITIES.items():
            weather = get_weather_data(city, coords['lat'], coords['lon'])
            producer.send(TOPIC, value=weather)
            print(f"✅ Sent {weather['mode']} weather for {city}: {weather['temperature']}°C")
        
        # Wait 20 seconds before next update to stay within API limits
        time.sleep(20)
except KeyboardInterrupt:
    print("Stopping Producer...")
finally:
    producer.close()