from fastapi import FastAPI
from influxdb_client import InfluxDBClient
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Enable CORS so your browser can talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "dDKt36p-oYnheiM38fqTR5dlTzht9j7r4jg6yG9hhodSZrnRIrlZUO9Ie3sHHEBKq70ggbNvza0mFqT39IDLkw=="  # Replace with your generated token
INFLUX_ORG = "space_org"
INFLUX_BUCKET = "solar_data"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()
import math
from datetime import datetime

@app.get("/api/earth-position")
def get_earth_coords():
    # 1. Get Day of Year (April 7, 2026 is day 97)
    now = datetime.now()
    day_of_year = now.timetuple().tm_yday
    
    # 2. Approximate Heliocentric Longitude (L)
    # Earth moves ~0.9856 degrees per day. 
    # On Winter Solstice (Dec 21), it's at ~90 degrees in this coordinate system.
    angle_deg = (day_of_year + 10) * 0.9856 
    angle_rad = math.radians(angle_deg)
    
    # 3. Apply to our Dashboard's Ellipse (25x16 scale)
    x = math.cos(angle_rad) * 32
    z = math.sin(angle_rad) * 22
    
    return {"x": x, "z": z, "angle": angle_deg}

@app.get("/api/flux-history")
def get_flux_history(minutes: int = 60):
    # Query InfluxDB for a range of data
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: -5m)          // narrow window — only care about recent data
    |> filter(fn: (r) => r._measurement == "solar_flux")
    |> filter(fn: (r) => r._field == "flux_value")
    |> group()
    |> sort(columns: ["_time"], desc: true)
    |> limit(n: 1)
    '''
    tables = query_api.query(query)
    
    history = []
    for table in tables:
        for record in table.records:
            history.append({
                "time": record.get_time().strftime("%H:%M:%S"),
                "flux": record.get_value()
            })
    return history
@app.get("/api/live-flux")
def get_live_flux():
    # Flux query to get the very last record
    query =f'''
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: -24h)
    |> filter(fn: (r) => r._measurement == "solar_flux")
    |> filter(fn: (r) => r._field == "flux_value")
    |> group() 
    |> sort(columns: ["_time"], desc: true)
    |> limit(n: 1)
'''
    tables = query_api.query(query)
    
    if not tables:
        return {"flux": 1e-7, "status": "No Data"}

    record = tables[0].records[0]
    flux = record.get_value()
    
    # Simple classification logic for the UI
    status = "Normal"
    if flux >= 1e-4: status = "X-Class"
    elif flux >= 1e-5: status = "M-Class"
    elif flux >= 1e-6: status = "C-Class"
    
    return {
        "flux": flux,
        "status": status,
        "time": record.get_time().strftime("%Y-%m-%d %H:%M:%S")
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)