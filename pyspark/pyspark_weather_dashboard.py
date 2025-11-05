import sys
import os
from datetime import datetime
import pytz

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, to_date, min as spark_min, max as spark_max, avg as spark_avg,
    lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, DateType
)

# so we can import your db file
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database_utils import (
    get_weather_from_postgres,
    insert_daily_report_to_postgres,
    insert_monthly_avg_status_to_postgres,
    insert_monthly_extremes_to_postgres,
    get_daily_reports_for_month,
    DB_CONFIG
)

TZ = "Europe/Copenhagen"

# define an explicit empty schema once
EMPTY_WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("recorded_at", TimestampType(), True),
    StructField("temperature_celsius", DoubleType(), True),
    StructField("wind_speed_ms", DoubleType(), True),
    StructField("weather_status", StringType(), True),
    StructField("recorded_date", DateType(), True),
])


def load_weather_df(spark):
    """Load data directly from the database without recalculating weather_status"""
    data = get_weather_from_postgres()
    if not data:
        print("No data retrieved from database.")
        # use the struct schema instead of string
        return spark.createDataFrame([], schema=EMPTY_WEATHER_SCHEMA)

    df = spark.createDataFrame([Row(**r) for r in data])

    df = (
        df
        .withColumn("temperature_celsius", col("temperature_celsius").cast("double"))
        .withColumn("wind_speed_ms", col("wind_speed_ms").cast("double"))
        .withColumn("recorded_date", to_date(col("recorded_at")))
    )

    return df

def compute_daily_stats(df, target_date_str):
    """min/max/avg per city for given date (string YYYY-MM-DD)"""
    return (
        df.filter(col("recorded_date") == lit(target_date_str))
          .groupBy("city")
          .agg(
              spark_min("temperature_celsius").alias("min_temp"),
              spark_max("temperature_celsius").alias("max_temp"),
              spark_avg("temperature_celsius").alias("avg_temp")
          )
          .withColumn("report_date", lit(target_date_str))
    )


def compute_monthly_analytics(df, year=None, month=None):
    """Compute monthly analytics:
       - avg temperature by weather status (from raw data)
       - min/max temperatures (from daily_report)"""
    # guard: if df is empty, no need to go further
    if not df.take(1):
        print("No data available in DataFrame.")
        return None, None, None, None

    tz = pytz.timezone(TZ)
    now = datetime.now(tz)
    year = year or now.year
    month = month or now.month
    report_month = f"{year}-{month:02}-01"

    # === Average by weather status ===
    df_month = df.filter((col("recorded_at").substr(1, 7) == f"{year}-{month:02}"))
    avg_by_status_df = (
        df_month.groupBy("weather_status")
        .agg(spark_avg("temperature_celsius").alias("avg_temp"))
    )
    avg_list = [
        {"weather_status": r["weather_status"], "avg_temp": float(r["avg_temp"])}
        for r in avg_by_status_df.collect()
    ]

    # === Extremes (from daily_report) ===
    daily_rows = get_daily_reports_for_month(year, month)
    if not daily_rows:
        print("No daily data for month -> skipping monthly extremes.")
        return report_month, avg_list, None, None

    # Find monthly min/max from daily aggregates
    min_day = min(daily_rows, key=lambda r: (r["min_temp"] if r["min_temp"] is not None else 9999))
    max_day = max(daily_rows, key=lambda r: (r["max_temp"] if r["max_temp"] is not None else -9999))

    min_row = {
        "city_id": min_day["city_id"],
        "temperature_celsius": float(min_day["min_temp"]),
        "recorded_at": min_day["report_date"],
        "city": min_day["city"]
    }

    max_row = {
        "city_id": max_day["city_id"],
        "temperature_celsius": float(max_day["max_temp"]),
        "recorded_at": max_day["report_date"],
        "city": max_day["city"]
    }

    return report_month, avg_list, min_row, max_row


def main():
    # make Spark use the same python as this process (helps on Windows)
    spark = (
        SparkSession.builder
        .appName("WeatherDashboard")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("Starting Weather report job...")

    df = load_weather_df(spark)

    # ---------- DAILY ----------
    tz = pytz.timezone(TZ)
    today_str = datetime.now(tz).date().isoformat()
    print(f"Calculates daily report for {today_str}")

    # this is safe even if df is empty: compute_daily_stats will return empty df
    daily_df = compute_daily_stats(df, today_str)

    # collect only if there are rows
    daily_rows = daily_df.collect() if not daily_df.rdd.isEmpty() else []

    # fetch city ids
    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM city;")
    city_map = {name: cid for cid, name in cur.fetchall()}
    cur.close()
    conn.close()

    daily_list = []
    for r in daily_rows:
        city_name = r["city"]
        city_id = city_map.get(city_name)
        if city_id is None:
            continue
        daily_list.append({
            "report_date": r["report_date"],
            "city_id": city_id,
            "min_temp": float(r["min_temp"]) if r["min_temp"] is not None else None,
            "max_temp": float(r["max_temp"]) if r["max_temp"] is not None else None,
            "avg_temp": float(r["avg_temp"]) if r["avg_temp"] is not None else None,
        })


    insert_daily_report_to_postgres(daily_list)

    # ---------- MONTHLY ----------
    print("Calculates monthly statistics...")
    report_month, avg_list, min_row, max_row = compute_monthly_analytics(df)
    if report_month:
        insert_monthly_avg_status_to_postgres(report_month, avg_list or [])
        if min_row and max_row:
            insert_monthly_extremes_to_postgres(report_month, min_row, max_row)
        else:
            print("No daily data found — skipping extreme values.")
    else:
        print("No valid month found — skipping monthly update.")

    spark.stop()
    print("Report completed. Data updated in the database.")


if __name__ == "__main__":
    main()
