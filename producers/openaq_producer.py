import json
import time
import random
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "openaq_air_quality"

print(f"🚀 Starting Synchronized Air Producer on topic: {TOPIC}")

while True:
    # SYNC STEP: Use ISO format to match weather producer
    now = datetime.datetime.now()
    clean_timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    data = {
        "city": "Paris",
        "value": round(random.uniform(5.0, 120.0), 2), # PM2.5 particles
        "timestamp": clean_timestamp
    }
    
    producer.send(TOPIC, data)
    print(f"✅ Sent Air Quality: {data}")
    
    time.sleep(30)