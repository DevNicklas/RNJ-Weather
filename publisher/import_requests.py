import requests
import json
import time
import os
import csv
import threading
import signal
from datetime import datetime, timedelta
from queue import Queue, Empty
import paho.mqtt.client as mqtt
from requests.adapters import HTTPAdapter, Retry

# -----------------------------
# Configuration
# -----------------------------
API_KEY = "725ca2bd-58f6-4269-90b5-759f207277ec"
COLLECTION = "harmonie_dini_sf"
MQTT_BROKER = "10.101.186.200"
MQTT_PORT = 1883
MQTT_TOPIC = "weather/data"
parameters = "temperature-0m,wind-speed-10m,precipitation-type"

precip_map = {
    0: "Drizzle", 1: "Rain", 2: "Sleet", 3: "Snow",
    4: "Freezing drizzle", 5: "Freezing rain", 6: "Graupel", 7: "Hail"
}

# -----------------------------
# Load cities from CSV
# -----------------------------
csv_file_path = os.path.join(os.path.dirname(__file__), "dk.csv")
locations = []

with open(csv_file_path, newline='', encoding='latin1') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        city = row['city']
        lat = float(row['lat'])
        lon = float(row['lng'])
        locations.append((city, lat, lon))

# -----------------------------
# Logging (thread-safe)
# -----------------------------
log_file_path = os.path.join(os.path.dirname(__file__), "log.csv")
log_lock = threading.Lock()

if not os.path.exists(log_file_path):
    with open(log_file_path, "w", newline="", encoding="utf-8") as logfile:
        writer = csv.writer(logfile)
        writer.writerow(["timestamp", "event", "details"])

def log_event(event, details=""):
    timestamp = datetime.utcnow().strftime("%d/%m-%Y-%H:%M:%S")
    print(f"{timestamp} | {event} | {details}")
    with log_lock:
        with open(log_file_path, "a", newline="", encoding="utf-8") as logfile:
            writer = csv.writer(logfile)
            writer.writerow([timestamp, event, details])

# -----------------------------
# API session with retries
# -----------------------------
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500,502,503,504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# -----------------------------
# MQTT setup
# -----------------------------
mqtt_connected = False
mqtt_client = mqtt.Client(client_id="python_weather_publisher")

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        log_event("MQTT Connected", f"{MQTT_BROKER}:{MQTT_PORT}")
    else:
        mqtt_connected = False
        log_event("MQTT Connection Failed", f"Return code: {rc}")

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    log_event("MQTT Disconnected", f"Return code: {rc}")

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

# -----------------------------
# Graceful shutdown
# -----------------------------
stop_event = threading.Event()

def shutdown(signum, frame):
    log_event("Shutdown", "Exiting gracefully")
    stop_event.set()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# -----------------------------
# MQTT Loop Thread
# -----------------------------
def mqtt_loop():
    global mqtt_connected
    reconnect_delay = 5
    while not stop_event.is_set():
        if not mqtt_connected:
            try:
                log_event("MQTT Connect Attempt", f"Trying {MQTT_BROKER}:{MQTT_PORT}")
                mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
                mqtt_client.loop_start()
                reconnect_delay = 5  # reset delay after success
            except Exception as e:
                log_event("MQTT Connect Failed", f"{e}")
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)  # exponential backoff
        time.sleep(1)
# -----------------------------
# Thread-safe queue for data
# -----------------------------
data_queue = Queue(maxsize=100)

# -----------------------------
# Fetching Thread
# -----------------------------
def fetch_loop():
    global mqtt_connected
    while not stop_event.is_set():
        # Wait until MQTT is connected before fetching
        if not mqtt_connected:
            log_event("Fetch Paused", "MQTT not connected, waiting to retry...")
            while not mqtt_connected and not stop_event.is_set():
                time.sleep(2)
            continue  # recheck before next cycle

        start_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(hours=1)
        datetime_range = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"

        for city_name, lat, lon in locations:
            # Stop fetching if disconnected mid-loop
            if stop_event.is_set() or not mqtt_connected:
                log_event("Fetch Stopped", "MQTT disconnected during city loop")
                break

            url = (
                f"https://dmigw.govcloud.dk/v1/forecastedr/collections/{COLLECTION}/position"
                f"?coords=POINT({lon}%20{lat})"
                f"&crs=crs84"
                f"&parameter-name={parameters}"
                f"&datetime={datetime_range}"
                f"&api-key={API_KEY}"
            )

            try:
                response = session.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                log_event("API Fetch Failed", f"{city_name}: {e}")
                continue

            ranges = data.get("ranges", {})
            temp_k = ranges.get("temperature-0m", {}).get("values", [0])[0] or 0
            wind = ranges.get("wind-speed-10m", {}).get("values", [0])[0] or 0
            precip_code = int(ranges.get("precipitation-type", {}).get("values", [0])[0] or 0)

            try:
                temp_c = round(temp_k - 273.15)
                wind = round(wind, 1)
                precip_status = precip_map.get(precip_code, "Unknown")
            except Exception:
                log_event("API Data Invalid", f"{city_name}: {data}")
                continue

            updated_time = datetime.utcnow().strftime("%d/%m-%Y-%H:%M")
            print(f"{city_name}: {temp_c}°C, {wind} m/s, {precip_status}, updated {updated_time}")
            print("-" * 40)

            forecast_data = {
                "city": city_name,
                "latitude": lat,
                "longitude": lon,
                "temperature": temp_c,
                "wind_speed": wind,
                "precipitation": precip_status,
                "updated_time": updated_time
            }

            try:
                data_queue.put_nowait(forecast_data)
            except:
                log_event("Queue Full", f"Dropping data for {city_name}")

            stop_event.wait(0.3)

# -----------------------------
# Publishing Thread
# -----------------------------
def publish_loop():
    global mqtt_connected
    while not stop_event.is_set():
        if not mqtt_connected:
            time.sleep(2)
            continue
        try:
            forecast_data = data_queue.get(timeout=5)
        except Empty:
            continue
        try:
            result = mqtt_client.publish(MQTT_TOPIC, json.dumps(forecast_data))
            if result.rc != 0:
                mqtt_connected = False
                log_event("MQTT Publish Failed", f"{forecast_data['city']} RC={result.rc}")
        except Exception as e:
            mqtt_connected = False
            log_event("MQTT Publish Exception", f"{forecast_data['city']}: {e}")

# -----------------------------
# Run threads
# -----------------------------
threading.Thread(target=mqtt_loop, daemon=True).start()
threading.Thread(target=fetch_loop, daemon=True).start()
threading.Thread(target=publish_loop, daemon=True).start()

# Keep main alive
while not stop_event.is_set():
    time.sleep(1)
