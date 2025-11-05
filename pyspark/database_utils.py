import psycopg2
import json
from contextlib import contextmanager
import os


# ========== DATABASE CONFIG ==========
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "10.101.17.71"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "weather"),
    "user": os.getenv("DB_USER", "kafka_user"),
    "password": os.getenv("DB_PASSWORD", "RNJ#Database")
}


# ========== DATABASE CONTEXT MANAGER ==========
@contextmanager
def get_db_cursor():
    """Context manager for PostgreSQL connection + cursor"""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print("Database operation failed:", e)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ========== INSERT WEATHER DATA ==========
def insert_to_postgres(json_input):
    """Insert a single JSON weather record into PostgreSQL"""
    data = json.loads(json_input) if isinstance(json_input, str) else json_input
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CALL add_weather_data(%s, %s, %s, %s, %s, %s, %s);
            """, (
                data["city"],
                data["recorded_at"],
                data["temperature_celsius"],
                data["wind_speed_ms"],
                data["weather_status"],
                data["latitude"],
                data["longitude"]
            ))

        print(f"Inserted weather data for {data['city']}")
    except Exception as e:
        print("Database insert error:", e)


# ========== GETTERS ==========
def get_weather_from_postgres():
    """Fetch all weather data from PostgreSQL using get_weather_data()"""
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM get_weather_data();")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print("Database fetch error:", e)
        return []


# ========== INSERT REPORT DATA ==========
def insert_daily_report_to_postgres(report_list):
    if not report_list:
        print("No daily report data to insert.")
        return

    try:
        with get_db_cursor() as cur:
            for r in report_list:
                cur.execute("""
                    CALL insert_daily_report(%s, %s, %s, %s, %s);
                """, (
                    r["report_date"],
                    r["min_temp"],
                    r["max_temp"],
                    r["avg_temp"],
                    r["city_id"]
                ))
        print(f"Inserted/updated {len(report_list)} daily report rows via procedure.")
    except Exception as e:
        print("Daily report procedure call error:", e)

        
def get_daily_reports_for_month(year: int, month: int):
    """Return all daily_report rows for the given month."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT dr.report_date, dr.city_id, c.name AS city,
                       dr.min_temp, dr.max_temp, dr.avg_temp
                FROM daily_report dr
                JOIN city c ON dr.city_id = c.id
                WHERE EXTRACT(YEAR FROM dr.report_date) = %s
                  AND EXTRACT(MONTH FROM dr.report_date) = %s;
            """, (year, month))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        print("get_daily_reports_for_month error:", e)
        return []

def insert_monthly_avg_status_to_postgres(report_month, avg_list):
    if not avg_list:
        print("No avg_by_status data to insert.")
        return

    try:
        with get_db_cursor() as cur:
            for row in avg_list:
                cur.execute("""
                    INSERT INTO monthly_avg_by_status (report_month, weather_status, avg_temp)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (report_month, weather_status)
                    DO UPDATE SET avg_temp = EXCLUDED.avg_temp;
                """, (report_month, row["weather_status"], row["avg_temp"]))
        print(f"Inserted/updated {len(avg_list)} avg_by_status rows for {report_month}.")
    except Exception as e:
        print("monthly_avg_by_status insert error:", e)


def insert_monthly_extremes_to_postgres(report_month, min_row, max_row):
    """Call the PostgreSQL stored procedure to insert/update monthly extremes."""
    if not min_row or not max_row:
        print("Missing min or max data.")
        return

    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CALL insert_monthly_extremes(%s, %s, %s, %s, %s, %s, %s);
            """, (
                report_month,
                min_row["city_id"], min_row["temperature_celsius"], min_row["recorded_at"],
                max_row["city_id"], max_row["temperature_celsius"], max_row["recorded_at"]
            ))
        print(f"Monthly extremes stored or updated for {report_month}.")
    except Exception as e:
        print("monthly_extremes procedure call error:", e)
