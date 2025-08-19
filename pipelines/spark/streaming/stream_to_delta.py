from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import col, from_json, to_timestamp, expr, window
from py4j.protocol import Py4JJavaError

import conf as C

# Schemas

bike_schema = StructType([
    StructField("city_id",    StringType(),  True),
    StructField("station_id", StringType(),  True),
    StructField("event_time", StringType(),  True),   # will parse -> timestamp
    StructField("bike_count", IntegerType(), True),
])

weather_schema = StructType([
    StructField("city_id",    StringType(),  True),
    StructField("event_time", StringType(),  True),   # will parse -> timestamp
    StructField("temp_c",     DoubleType(),  True),
    StructField("precip_mm",  DoubleType(),  True),
])


def build_spark():
    b = SparkSession.builder.appName("mobility-stream")

    # Delta + S3A/MinIO opts from  conf.py
    for k, v in C.DELTA_OPTS.items():
        b = b.config(k, v)

    
    b = b.config(
        "spark.jars.packages",
        ",".join([
            # Required for structured streaming ↔ Kafka
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1",
        ])
    )

    b = b.config("spark.sql.adaptive.enabled", "false")

    spark = b.getOrCreate()

    try:
        spark._jvm.java.lang.Class.forName("org.apache.spark.kafka010.KafkaConfigUpdater")
    except Py4JJavaError:
        raise RuntimeError(
            "\n[Classpath error] Missing Spark Kafka integration jar.\n"
            "Spark could not load org.apache.spark.kafka010.KafkaConfigUpdater.\n\n"
            "You must add these jars to the driver & executors classpath BEFORE starting the app:\n"
            "  - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1\n"
            "  - org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1\n"
            "Plus transitive deps typically needed in the image:\n"
            "  - org.apache.kafka:kafka-clients:3.5.x\n"
            "  - org.apache.commons:commons-pool2:2.x\n\n"
            "If your containers are offline, do NOT rely on spark.jars.packages.\n"
            "Bake the jars into the image (e.g., /opt/bitnami/spark/jars) or mount them and set SPARK_CLASSPATH.\n"
        )

    return spark

# ----------------------------
# IO helpers
# ----------------------------
def read_kafka_json(spark, topic: str, schema: StructType):
    kafka = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", C.REDPANDA_BROKERS)
             .option("subscribe", topic)
             .option("startingOffsets", "latest")
             .option("failOnDataLoss", "false")
             .load()
    )
    parsed = (
        kafka.selectExpr("CAST(value AS STRING) AS json_str")
             .select(from_json(col("json_str"), schema).alias("data"))
             .select("data.*")
    )
    return parsed

def start_delta_sink(df, path: str, checkpoint: str, mode: str = "append"):
    return (
        df.writeStream
          .format("delta")
          .outputMode(mode)
          .option("checkpointLocation", checkpoint)
          .option("mergeSchema", "true")
          .start(path)
    )

# ----------------------------
# Main pipeline
# ----------------------------
def main():
    spark = build_spark()

    # ---- Bronze ----
    bike_raw = (
        read_kafka_json(spark, C.TOPIC_BIKE, bike_schema)
        .withColumn("event_time_b", to_timestamp(col("event_time")))
        .drop("event_time")
        .withWatermark("event_time_b", "15 minutes")
    )

    weather_raw = (
        read_kafka_json(spark, C.TOPIC_WEATHER, weather_schema)
        .withColumn("event_time_w", to_timestamp(col("event_time")))
        .drop("event_time")
        .withWatermark("event_time_w", "15 minutes")
    )

    q_bronze_bike = start_delta_sink(
        bike_raw, f"{C.BRONZE}/bike", f"{C.CHECKPOINTS}/bronze_bike", "append"
    )
    q_bronze_weather = start_delta_sink(
        weather_raw, f"{C.BRONZE}/weather", f"{C.CHECKPOINTS}/bronze_weather", "append"
    )

    # ---- Silver (join + 5-min window agg) ----
    joined = (
        bike_raw.alias("b")
        .join(
            weather_raw.alias("w"),
            expr("""
                 b.city_id = w.city_id
             AND b.event_time_b BETWEEN w.event_time_w - INTERVAL 15 MINUTES
                                     AND w.event_time_w + INTERVAL 15 MINUTES
            """),
            "leftOuter"
        )
    )

    silver_agg = (
        joined.groupBy(
            window(col("b.event_time_b"), "5 minutes").alias("w5"),
            col("b.city_id").alias("city_id"),
        )
        .agg(
            expr("sum(b.bike_count)  as total_bike_count"),
            expr("avg(w.temp_c)      as avg_temp_c"),
            expr("avg(w.precip_mm)   as avg_precip_mm"),
        )
        .select(
            col("city_id"),
            col("w5.start").alias("window_start"),
            col("w5.end").alias("window_end"),
            col("total_bike_count"),
            col("avg_temp_c"),
            col("avg_precip_mm"),
        )
    )

    q_silver = start_delta_sink(
        silver_agg,
        f"{C.SILVER}/mobility_weather_5min",
        f"{C.CHECKPOINTS}/silver_mobility_weather_5min",
        "append",
    )

    # ---- Gold (curated projection) ----
    gold = silver_agg.select(
        "city_id", "window_start", "window_end",
        "total_bike_count", "avg_temp_c", "avg_precip_mm"
    )

    q_gold = start_delta_sink(
        gold,
        f"{C.GOLD}/mobility_weather_5min",
        f"{C.CHECKPOINTS}/gold_mobility_weather_5min",
        "append",
    )

    # ---- Gold (station availability, 5-minute averages) ----
    # Per-station average bikes in 5-minute windows, grouped by city and station.
    station_5min = (
        bike_raw.groupBy(
            window(col("event_time_b"), "5 minutes").alias("w5"),
            col("city_id"),
            col("station_id"),
        )
        .agg(expr("avg(bike_count) as avg_bikes"))
        .select(
            col("city_id"),
            col("station_id"),
            col("w5.start").alias("window_start"),
            col("w5.end").alias("window_end"),
            col("avg_bikes"),
        )
    )

    q_gold_station = start_delta_sink(
        station_5min,
        f"{C.GOLD}/station_availability_5min",
        f"{C.CHECKPOINTS}/gold_station_availability_5min",
        "append",
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
