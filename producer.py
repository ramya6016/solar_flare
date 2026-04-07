from datetime import datetime, timedelta, timezone
import requests
import json
import time
import sys
from kafka import KafkaProducer

# Configuration
API_URL = "https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json"
KAFKA_BROKER = 'localhost:19092'
TOPIC = 'solar-flux'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- HISTORICAL DATASET (May 2024 Storm Simulation) ---
# --- HISTORICAL DATASET (Extreme Test Injection) ---
historical_data = [
  # X-CLASS (EXTREME)
    {"time_tag": time.time(), "flux": 0.0009, "energy": "1-8 Angstrom"},  # X-CLASS (STILL HIGH)
]

def run_live_mode():
    headers = {'User-Agent': 'Mozilla/5.0'} # Pretend to be a browser
    try:
        response = requests.get(API_URL, headers=headers)
        
        # Check if the request actually worked (200 OK)
        if response.status_code != 200:
            print(f"❌ Server returned error: {response.status_code}")
            return

        # Check if the content is empty
        if not response.text:
            print("❌ Empty response from satellite feed.")
            return

        data = response.json()
        latest_reading = data[-1]
        print(f"📡 Latest Reading: {latest_reading['time_tag']} | Energy: {latest_reading['energy']} | Flux: {latest_reading['flux']}")
        if "0.8" in latest_reading.get("energy") or "1-8" in latest_reading.get("energy"):
            producer.send(TOPIC, latest_reading)
            producer.flush()  # Ensure the message is sent immediately
            print(f"✅ Sent: {latest_reading['time_tag']} | Flux: {latest_reading['flux']}")
            
    except json.decoder.JSONDecodeError:
        print(response.text)  # Print the raw response for debugging
        print("❌ Received non-JSON data. The API might be down for maintenance.")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")


def run_historical_mode():
    print("📂 Mode: EXTREME TEST Replay (X-Class Injection)")
    while True:
        for record in historical_data:
            current_time_str = datetime.now(timezone.utc).isoformat()
            record['time_tag'] = current_time_str
            producer.send(TOPIC, record)
            producer.flush()
            print(f"🔥 Testing Flux: {record['flux']} | Time: {record['time_tag']}")
            time.sleep(3)
        
        # If you want a "cooldown" Normal reading between cycles, 
        # give it its own sleep and flush
          # ← give dashboard time to show Normal before X-Class floods again

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--historical":
        run_historical_mode()
    else:
        while True:
            run_live_mode()
            time.sleep(60)