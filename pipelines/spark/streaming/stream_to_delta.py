# /opt/spark/workdir/streaming/stream_to_delta.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    to_date,
    sum as _sum,
    avg as _avg,
    max as _max,
    expr,
    round as _round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

import conf as C  # do not touch your delta opts

# ----------- Config (env) -----------
BROKER = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC_BIKE = os.getenv("KAFKA_TOPIC_BIKE", "bike")
TOPIC_WEATHER = os.getenv("KAFKA_TOPIC_WEATHER", "weather")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
S3_BASE = f"s3a://{S3_BUCKET}"

# Layout in the bucket
BRONZE_BIKE = f"{S3_BASE}/bronze/bike"
BRONZE_WEATHER = f"{S3_BASE}/bronze/weather"
SILVER_BIKE = f"{S3_BASE}/silver/bike_events"
SILVER_WEATHER = f"{S3_BASE}/silver/weather_events"
GOLD_BIKE_HOURLY = f"{S3_BASE}/gold/bike_usage_hourly"
GOLD_BIKE_DAILY = f"{S3_BASE}/gold/bike_usage_daily"
GOLD_WEATHER_HR = f"{S3_BASE}/gold/weather_hourly"
GOLD_WEATHER_DY = f"{S3_BASE}/gold/weather_daily"
GOLD_BIKE_MINUTE = f"{S3_BASE}/gold/bike_minute"

# Checkpoints
CKPT_BASE = f"{S3_BASE}/_checkpoints"
CKPT_BRONZE_BIKE = f"{CKPT_BASE}/bronze_bike"
CKPT_BRONZE_WEATHER = f"{CKPT_BASE}/bronze_weather"
CKPT_SILVER_BIKE = f"{CKPT_BASE}/silver_bike"
CKPT_SILVER_WEATHER = f"{CKPT_BASE}/silver_weather"
CKPT_GOLD_BIKE_H = f"{CKPT_BASE}/gold_bike_hourly"
CKPT_GOLD_BIKE_D = f"{CKPT_BASE}/gold_bike_daily"
CKPT_GOLD_WX_H = f"{CKPT_BASE}/gold_weather_hourly"
CKPT_GOLD_WX_D = f"{CKPT_BASE}/gold_weather_daily"
CKPT_GOLD_BIKE_M = f"{CKPT_BASE}/gold_bike_minute"

# ----------- Schemas (match producers exactly) -----------
bike_schema = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("bike_city", StringType(), True),
        StructField("docks_total", IntegerType(), True),  # capacity
        StructField("bikes_available", IntegerType(), True),  # current available bikes
        StructField("bikes_occupied", IntegerType(), True),  # currently taken/occupied
        StructField("docks_available", IntegerType(), True),  # mirrors availability now
        StructField("event_time", StringType(), True),  # ISO8601 string
    ]
)

weather_schema = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("weather_city", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("precip_mm", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_kph", DoubleType(), True),
        StructField("pressure_mb", IntegerType(), True),
        StructField("condition", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)


# ----------- Spark session -----------
def build_spark():
    b = SparkSession.builder.appName("CityMobilityPulse-ETL")
    for k, v in C.DELTA_OPTS.items():
        b = b.config(k, v)

    # keep Kafka integration here (does not change your delta opts)
    b = b.config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1",
    ).config("spark.sql.adaptive.enabled", "false")

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------- IO helpers -----------
def read_kafka_stream(spark, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_delta(
    df, path: str, checkpoint: str, mode: str = "append", partition_cols=None
):
    w = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode(mode)
    )
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    return w.start(path)


# ----------- Main -----------
def main():
    spark = build_spark()

    # -------- BRONZE (raw JSON + Kafka metadata) --------
    df_bike_raw = (
        read_kafka_stream(spark, TOPIC_BIKE)
        .select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType").alias("kafka_timestamp_type"),
            expr("current_timestamp()").alias("ingest_time"),
        )
        .withColumn("dt", to_date(col("ingest_time")))
    )
    q_bronze_bike = write_delta(
        df_bike_raw, BRONZE_BIKE, CKPT_BRONZE_BIKE, partition_cols=["dt"]
    )

    df_weather_raw = (
        read_kafka_stream(spark, TOPIC_WEATHER)
        .select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType").alias("kafka_timestamp_type"),
            expr("current_timestamp()").alias("ingest_time"),
        )
        .withColumn("dt", to_date(col("ingest_time")))
    )

    q_bronze_weather = write_delta(
        df_weather_raw, BRONZE_WEATHER, CKPT_BRONZE_WEATHER, partition_cols=["dt"]
    )

    # ---------- 2) SILVER: parse & clean ----------

    # Bike silver: parse JSON, cast types, proper event_time (timestamp), helper cols
    df_bike_silver = (
        df_bike_raw.select(from_json(col("value"), bike_schema).alias("j"))
        .where(col("j").isNotNull())  # drop malformed messages
        .select("j.*")
        .withColumn("event_time", to_timestamp(col("event_time")))  # ISO handled
        .withColumn("dt", expr("to_date(event_time)"))
        .withWatermark("event_time", "15 minutes")
        # Derived metrics (null-safe; avoid div-by-zero)
        .withColumn(
            "availability_rate",
            expr(
                "CASE WHEN docks_total IS NOT NULL AND docks_total > 0 THEN bikes_available / docks_total ELSE NULL END"
            ),
        )
        .withColumn(
            "utilization_rate",
            expr(
                "CASE WHEN docks_total IS NOT NULL AND docks_total > 0 THEN bikes_occupied / docks_total ELSE NULL END"
            ),
        )
    )

    q_silver_bike = (
        df_bike_silver.writeStream.format("delta")
        .option("checkpointLocation", CKPT_SILVER_BIKE)
        .partitionBy("dt", "station_id")
        .outputMode("append")
        .start(SILVER_BIKE)
    )

    # Weather silver: parse JSON and cast
    df_weather_silver = (
        df_weather_raw.select(from_json(col("value"), weather_schema).alias("j"))
        .where(col("j").isNotNull())
        .select("j.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withColumn("dt", expr("to_date(event_time)"))
        .withWatermark("event_time", "15 minutes")
    )

    q_silver_weather = (
        df_weather_silver.writeStream.format("delta")
        .option("checkpointLocation", CKPT_SILVER_WEATHER)
        .partitionBy("dt", "station_id")
        .outputMode("append")
        .start(SILVER_WEATHER)
    )

    # ---------- 3) GOLD: BIKE aggregations ----------

    # Hourly per station & city
    bike_hourly = (
        df_bike_silver.groupBy(
            window(col("event_time"), "1 hour").alias("w"),
            col("station_id"),
            col("bike_city"),
        )
        .agg(
            _sum("bikes_occupied").alias("bikes_occupied_hour"),
            _avg("bikes_available").alias("bikes_available_avg_hour"),
            _avg("availability_rate").alias("availability_rate_avg_hour"),
            _avg("utilization_rate").alias("utilization_rate_avg_hour"),
            _max("docks_total").alias(
                "docks_total"
            ),  # stable per station, but safe to max()
        )
        .select(
            col("station_id"),
            col("bike_city"),
            col("w").getField("start").alias("window_start"),
            col("w").getField("end").alias("window_end"),
            col("bikes_occupied_hour"),
            col("bikes_available_avg_hour"),
            col("availability_rate_avg_hour"),
            col("utilization_rate_avg_hour"),
            col("docks_total"),
            expr("to_date(w.end)").alias("dt"),
        )
    )

    q_gold_bike_h = (
        bike_hourly.writeStream.format("delta")
        .option("checkpointLocation", CKPT_GOLD_BIKE_H)
        .partitionBy("dt", "station_id")
        .outputMode("append")
        .start(GOLD_BIKE_HOURLY)
    )

    # Daily per station & city
    bike_daily = (
        df_bike_silver.groupBy(
            window(col("event_time"), "1 day").alias("w"),
            col("station_id"),
            col("bike_city"),
        )
        .agg(
            _sum("bikes_occupied").alias("bikes_occupied_day"),
            _avg("bikes_available").alias("bikes_available_avg_day"),
            _avg("availability_rate").alias("availability_rate_avg_day"),
            _avg("utilization_rate").alias("utilization_rate_avg_day"),
            _max("docks_total").alias("docks_total"),
        )
        .select(
            col("station_id"),
            col("bike_city"),
            col("w").getField("start").alias("window_start"),
            col("w").getField("end").alias("window_end"),
            col("bikes_occupied_day"),
            col("bikes_available_avg_day"),
            col("availability_rate_avg_day"),
            col("utilization_rate_avg_day"),
            col("docks_total"),
            expr("to_date(w.end)").alias("dt"),
        )
    )

    q_gold_bike_d = (
        bike_daily.writeStream.format("delta")
        .option("checkpointLocation", CKPT_GOLD_BIKE_D)
        .partitionBy("dt", "station_id")
        .outputMode("append")
        .start(GOLD_BIKE_DAILY)
    )

    # ---------- 4) GOLD: WEATHER aggregations ----------

    # Hourly per station & city
    weather_hourly = (
        df_weather_silver.groupBy(
            window(col("event_time"), "1 hour").alias("w"),
            col("station_id"),
            col("weather_city"),
        )
        .agg(
            _avg("temp_c").alias("avg_temp_c_hour"),
            _sum("precip_mm").alias("sum_precip_mm_hour"),
            _avg("humidity").alias("avg_humidity_hour"),
            _avg("wind_kph").alias("avg_wind_kph_hour"),
            _avg("pressure_mb").alias("avg_pressure_mb_hour"),
        )
        .select(
            col("station_id"),
            col("weather_city"),
            col("w").getField("start").alias("window_start"),
            col("w").getField("end").alias("window_end"),
            col("avg_temp_c_hour"),
            col("sum_precip_mm_hour"),
            col("avg_humidity_hour"),
            col("avg_wind_kph_hour"),
            col("avg_pressure_mb_hour"),
            expr("to_date(w.end)").alias("dt"),
        )
    )

    q_gold_wx_h = (
        weather_hourly.writeStream.format("delta")
        .option("checkpointLocation", CKPT_GOLD_WX_H)
        .partitionBy("dt", "station_id")
        .outputMode("append")
        .start(GOLD_WEATHER_HR)
    )

    # Daily per station & city
    weather_daily = (
        df_weather_silver.groupBy(
            window(col("event_time"), "1 day").alias("w"),
            col("station_id"),
            col("weather_city"),
        )
        .agg(
            _avg("temp_c").alias("avg_temp_c_day"),
            _sum("precip_mm").alias("sum_precip_mm_day"),
            _avg("humidity").alias("avg_humidity_day"),
            _avg("wind_kph").alias("avg_wind_kph_day"),
            _avg("pressure_mb").alias("avg_pressure_mb_day"),
        )
        .select(
            col("station_id"),
            col("weather_city"),
            col("w").getField("start").alias("window_start"),
            col("w").getField("end").alias("window_end"),
            col("avg_temp_c_day"),
            col("sum_precip_mm_day"),
            col("avg_humidity_day"),
            col("avg_wind_kph_day"),
            col("avg_pressure_mb_day"),
            expr("to_date(w.end)").alias("dt"),
        )
    )

    q_gold_wx_d = (
        weather_daily.writeStream.format("delta")
        .option("checkpointLocation", CKPT_GOLD_WX_D)
        .partitionBy("dt", "station_id")
        .outputMode("append")
        .start(GOLD_WEATHER_DY)
    )
    # Run forever

    # ---------- 5) GOLD: BIKE minute-level snapshot (dev-friendly, near real-time) ----------
    # Tumbling window = 1 minute, watermark = 2 minutes.
    # We use AVG over the minute (rounded) + MAX(docks_total) as it is fixed per station.

    bike_minute = (
        df_bike_silver.groupBy(
            window(col("event_time"), "1 minute").alias("w"),
            col("station_id"),
            col("bike_city"),
        )
        .agg(
            _avg("bikes_available").alias("bikes_available_avg"),
            _avg("bikes_occupied").alias("bikes_occupied_avg"),
            _max("docks_total").alias("docks_total"),
        )
        .select(
            col("station_id"),
            col("bike_city"),
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            _round(col("bikes_available_avg")).cast("int").alias("bikes_available"),
            _round(col("bikes_occupied_avg")).cast("int").alias("bikes_occupied"),
            col("docks_total"),
            expr("to_date(window_start)").alias("dt"),
        )
    )

    q_gold_bike_m = (
        bike_minute.writeStream.format("delta")
        .option("checkpointLocation", CKPT_GOLD_BIKE_M)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .partitionBy("dt")
        .start(GOLD_BIKE_MINUTE)
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
