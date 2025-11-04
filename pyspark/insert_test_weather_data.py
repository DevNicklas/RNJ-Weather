import random
from datetime import datetime, timedelta
from database_utils import insert_to_postgres  # 👈 importer din egen funktion

# ========== GENERER TESTDATA ==========
WEATHER_STATUSES = [
    "Drizzle",
    "Rain",
    "Snow",
    "Freezing drizzle",
    "Freezing rain",
    "Graupel",
    "Hail"
]

# ========== GENERER TESTDATA ==========
def generate_test_data():
    cities = [
        {"city": "Copenhagen", "country": "Denmark", "latitude": 55.6761, "longitude": 12.5683},
        {"city": "Aarhus", "country": "Denmark", "latitude": 56.1629, "longitude": 10.2039},
        {"city": "Odense", "country": "Denmark", "latitude": 55.4038, "longitude": 10.4024},
        {"city": "Aalborg", "country": "Denmark", "latitude": 57.0488, "longitude": 9.9217},
        {"city": "Esbjerg", "country": "Denmark", "latitude": 55.4765, "longitude": 8.4594},
        {"city": "Ringsted", "country": "Denmark", "latitude": 55.4421, "longitude": 11.7900},
    ]

    now = datetime.now()
    start_date = now - timedelta(days=30)
    records = []

    for city in cities:
        for day in range(30):
            for hour in [0, 6, 12, 18]:  # 4 målinger pr. dag
                recorded_at = start_date + timedelta(days=day, hours=hour)

                record = {
                    "city": city["city"],
                    "latitude": city["latitude"],
                    "longitude": city["longitude"],
                    "recorded_at": recorded_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "temperature_celsius": round(random.uniform(-5, 25), 2),
                    "wind_speed_ms": round(random.uniform(0, 15), 2),
                    "weather_status": random.choice(WEATHER_STATUSES)  # ✅ now using ENUM values
                }
                records.append(record)
    return records


# ========== INDSÆT TESTDATA ==========
def insert_test_data():
    print("🌤️ Indsætter testdata i PostgreSQL...")
    records = generate_test_data()
    for record in records:
        insert_to_postgres(record)
    print(f"✅ {len(records)} vejrposter indsat i databasen!")


if __name__ == "__main__":
    insert_test_data()