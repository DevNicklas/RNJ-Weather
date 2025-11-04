from flask import Flask, render_template, jsonify, request
from datetime import date
import psycopg2

app = Flask(
    __name__,
    template_folder="website/templates",
    static_folder="website/static"
)

DB_HOST = "10.101.201.31"
DB_NAME = "weather"
DB_USER = "kafka_user"
DB_PASS = "RNJ#Database"

def get_cities():
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    cur.execute("SELECT c.name, c.latitude, c.longitude, c.id FROM city c")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"name": r[0], "lat": float(r[1]), "lon": float(r[2]), "id": int(r[3])} for r in rows]

def get_weather_data_for_graph(p_datetime, p_city_id: int = None):
    p_date = p_datetime.date() if hasattr(p_datetime, 'date') else p_datetime
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        sql = """
        SELECT
            c.id AS city_id,
            c.name AS city_name,
            (wd.recorded_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Copenhagen')::time AS recorded_hour,
            ROUND(AVG(wd.temperature_celsius), 2) AS avg_temp
        FROM weather_data wd
        JOIN weather w ON wd.id = w.weather_data_id
        JOIN city c ON w.city_id = c.id
        WHERE (wd.recorded_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Copenhagen')::date = %s
        """
        params = [p_date]
        if p_city_id is not None:
            sql += " AND c.id = %s"
            params.append(p_city_id)
        sql += " GROUP BY c.id, c.name, recorded_hour ORDER BY recorded_hour"
        cur.execute(sql, params)
        rows = cur.fetchall()
        hour_map = {row[2].hour: float(row[3]) if row[3] is not None else 0 for row in rows if row[2] is not None}
        return [{"recorded_hour": h, "avg_temp": hour_map.get(h), "avg_wind": None, "dominant_status": None} for h in range(24)]
    except Exception as e:
        print("Error fetching weather graph data:", e)
        return [{"recorded_hour": h, "avg_temp": 0, "avg_wind": None, "dominant_status": None} for h in range(24)]
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def get_monthly_report(p_year: int, p_month: int):
    """Call the PostgreSQL get_monthly_report function and return results as list of dicts."""
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cur = conn.cursor()
        cur.execute("SELECT * FROM get_monthly_report(%s, %s)", (p_year, p_month))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        data = []
        for row in rows:
            item = {}
            for i, col in enumerate(colnames):
                val = row[i]
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                item[col] = val
            data.append(item)
        return data
    except Exception as e:
        print("Error fetching monthly report:", e)
        return []
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route("/api/cities")
def api_cities():
    return jsonify(get_cities())

@app.route("/api/getgraphdata")
def api_getgraphdata():
    date_str = request.args.get("date")
    city_id = request.args.get("city_id")
    if not date_str:
        return jsonify({"error": "date parameter is required"}), 400
    try:
        p_date = date.fromisoformat(date_str)
    except ValueError:
        return jsonify({"error": "invalid date format, expected YYYY-MM-DD"}), 400
    p_city_id = int(city_id) if city_id else None
    return jsonify(get_weather_data_for_graph(p_date, p_city_id))

@app.route("/api/latest_weather_for_city")
def api_latest_weather():
    city_id = request.args.get("city_id")
    if not city_id:
        return jsonify({"error": "city_id parameter is required"}), 400
    try:
        city_id = int(city_id)
    except ValueError:
        return jsonify({"error": "invalid city_id"}), 400
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    cur.execute("""
        SELECT wd.id, wd.recorded_at, wd.temperature_celsius, wd.wind_speed_ms, wd.weather_status
        FROM weather_data wd
        JOIN weather w ON wd.id = w.weather_data_id
        WHERE w.city_id = %s
        ORDER BY wd.recorded_at DESC
        LIMIT 1
    """, (city_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return jsonify({"message": "no weather data found for this city"}), 404
    return jsonify({
        "id": row[0],
        "recorded_at": row[1].isoformat(),
        "temperature_celsius": float(row[2]),
        "wind_speed_ms": float(row[3]),
        "weather_status": row[4]
    })

@app.route("/api/monthly_report")
def api_monthly_report():
    year = request.args.get("year")
    month = request.args.get("month")
    if not year or not month:
        return jsonify({"error": "year and month parameters are required"}), 400
    try:
        year = int(year)
        month = int(month)
        if month < 1 or month > 12:
            raise ValueError("Month must be between 1 and 12")
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    data = get_monthly_report(year, month)
    if not data:
        return jsonify({"message": "No data found for the given month"}), 404
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
