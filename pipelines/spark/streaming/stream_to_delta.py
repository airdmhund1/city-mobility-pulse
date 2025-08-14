from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# from your conf.py
from conf import BRONZE, SILVER, GOLD, KAFKA_BROKERS, TOPIC_BIKE, TOPIC_WEATHER, CHECKPOINTS, DELTA_OPTS


def spark():
    """
    Build a SparkSession with Delta + S3A (MinIO) configured.
    Credentials/endpoint come from env vars with sensible MinIO defaults.
    """
    # Env-driven MinIO/S3A config (override these via env if needed)
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")  # e.g., "http://localhost:9000" if running locally
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    SSL_ENABLED = "true" if MINIO_ENDPOINT.strip().lower().startswith("https") else "false"

    builder = (
        SparkSession.builder.appName("mobility-stream")
        # Core Delta config (safe to set even if you also pass via DELTA_OPTS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Delta log store for object storage
        .config("spark.delta.logStore.class", "io.delta.storage.S3SingleDriverLogStore")
        # S3A (MinIO) settings
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", SSL_ENABLED)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        # Nice-to-haves for local/dev streaming
        .config("spark.sql.shuffle.partitions", "1")
    )

    # Apply any extra options you already defined in conf.DELTA_OPTS
    for k, v in DELTA_OPTS.items():
        builder = builder.config(k, v)

    return builder.getOrCreate()


def main():
    sp = spark()

    # Schemas
    bike_schema = StructType([
        StructField("station_id", StringType()),
        StructField("bikes_available", DoubleType()),
        StructField("docks_available", DoubleType()),
        StructField("event_time", StringType()),  # ISO8601 string
    ])
    weather_schema = StructType([
        StructField("temp_c", DoubleType()),
        StructField("precip_mm", DoubleType()),
        StructField("event_time", StringType()),
    ])

    # ---- Bronze (raw JSON from Kafka) ----
    bike_raw = (
        sp.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_BIKE)
        .option("startingOffsets", "latest")
        .load()
    )
    weather_raw = (
        sp.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_WEATHER)
        .option("startingOffsets", "latest")
        .load()
    )

    bike_bronze = bike_raw.selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_time")
    weather_bronze = weather_raw.selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_time")

    q_bike_bronze = (
        bike_bronze.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_bike")
        .outputMode("append")
        .start(f"{BRONZE}/bike")
    )
    q_weather_bronze = (
        weather_bronze.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_weather")
        .outputMode("append")
        .start(f"{BRONZE}/weather")
    )

    # ---- Silver (parsed, deduped, watermark) ----
    bike_silver = (
        bike_bronze
        .select(from_json(col("json"), bike_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .withWatermark("event_time", "15 minutes")
        .dropDuplicates(["station_id", "event_time"])
        .alias("b")
    )
    weather_silver = (
        weather_bronze
        .select(from_json(col("json"), weather_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .withWatermark("event_time", "15 minutes")
        .dropDuplicates(["event_time"])
        .alias("w")
    )

    q_bike_silver = (
        bike_silver.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_bike")
        .outputMode("append")
        .start(f"{SILVER}/bike")
    )
    q_weather_silver = (
        weather_silver.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_weather")
        .outputMode("append")
        .start(f"{SILVER}/weather")
    )

    # ---- Gold (joined aggregates) ----
    # Stream-stream left join with time bounds (requires watermarks on both sides, already set above)
    joined = bike_silver.join(
        weather_silver,
        expr("""
            w.event_time >= b.event_time - interval 10 minutes AND
            w.event_time <= b.event_time + interval 10 minutes
        """),
        how="leftOuter",
    )

    gold = (
        joined
        .groupBy("station_id", window(col("event_time"), "5 minutes"))
        .agg(
            expr("avg(bikes_available) as avg_bikes"),
            expr("avg(docks_available) as avg_docks"),
            expr("avg(temp_c) as avg_temp_c"),
            expr("avg(coalesce(precip_mm, 0)) as avg_precip_mm"),
        )
        .select(
            col("station_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_bikes", "avg_docks", "avg_temp_c", "avg_precip_mm",
        )
    )

    q_gold = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/gold_station")
        .outputMode("complete")
        .start(f"{GOLD}/station_availability")
    )

    sp.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
