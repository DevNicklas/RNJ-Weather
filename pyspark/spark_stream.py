from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
import os
import platform
from datetime import datetime


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database_utils import insert_to_postgres

def start_kafka_stream():
    # === Initialize Spark session with Kafka support ===
    is_windows = platform.system().lower().startswith("win")
    checkpoint_dir = (
        "file:///C:/tmp/spark_checkpoints/weather_kafka"
        if is_windows
        else "/tmp/spark_checkpoints/weather_kafka"
    )

    # === Windows ===
    if (is_windows):
        spark = (
            SparkSession.builder
            .appName("PySparkKafkaWeatherStream")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.local.dir", "C:/tmp/spark_local")
            .getOrCreate()
        )
    else:
    # === Linux  ===
        spark = (
        SparkSession.builder
        .appName("PySparkKafkaWeatherStream")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.local.dir", "/tmp/spark_local")
        .getOrCreate()
        )


    spark.sparkContext.setLogLevel("WARN")

    # === Kafka configuration ===
    kafka_brokers = "10.101.236.69:9092"   # KRaft broker address
    topic = "weather-data"

    # === Read messages from Kafka ===
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Kafka delivers key/value as bytes → convert value to string
    raw_values = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

    # === Define schema for incoming JSON messages ===
    weather_schema = StructType([
        StructField("city", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("updated_time", StringType()),
        StructField("temperature", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("precipitation", StringType())
    ])

    # Parse JSON into structured DataFrame
    parsed_stream = (
        raw_values
        .withColumn("data", from_json(col("json_str"), weather_schema))
        .select(
            col("data.city"),
            col("data.latitude"),
            col("data.longitude"),
            col("data.updated_time").alias("recorded_at"),
            col("data.temperature").alias("temperature_celsius"),
            col("data.wind_speed").alias("wind_speed_ms"),
            col("data.precipitation").alias("weather_status")
        )
        .withColumn("ingested_at", current_timestamp())
    )

    # === Process each batch ===
    def process_batch(batch_df, batch_id):
        count = batch_df.count()
        print(f"New Kafka Batch {batch_id} — {count} records received.")
        if count == 0:
            return

        records = [row.asDict() for row in batch_df.collect()]

        for record in records:
            updated_time = record.get("recorded_at")
            try:
                if updated_time:
                    record["recorded_at"] = datetime.strptime(updated_time, "%d/%m-%Y-%H:%M")
                else:
                    print(f"Missing updated_time for {record.get('city')} — using current time")
                    record["recorded_at"] = datetime.now()
            except ValueError:
                print(f"Invalid time format '{updated_time}' for {record.get('city')} — using current time")
                record["recorded_at"] = datetime.now()

            try:
                insert_to_postgres(record)
            except Exception as e:
                print(f"Failed to insert record for {record.get('city')}: {e}")

    # === Write stream and start listening ===
    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    print(f"Spark is now consuming from Kafka topic '{topic}' on {kafka_brokers} ...")
    query.awaitTermination()


if __name__ == "__main__":
    start_kafka_stream()
