from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from conf import BRONZE, SILVER, GOLD, KAFKA_BROKERS, TOPIC_BIKE, TOPIC_WEATHER, CHECKPOINTS, DELTA_OPTS


def spark():
    builder = SparkSession.builder.appName("mobility-stream")
    for k, v in DELTA_OPTS.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


def main():
    sp = spark()

    bike_schema = StructType([
        StructField("station_id", StringType()),
        StructField("bikes_available", DoubleType()),
        StructField("docks_available", DoubleType()),
        StructField("event_time", StringType())  # ISO8601 string
    ])
    weather_schema = StructType([
        StructField("temp_c", DoubleType()),
        StructField("precip_mm", DoubleType()),
        StructField("event_time", StringType())
    ])

    # ---- Bronze (raw JSON) ----
    bike_raw = (
        sp
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_BIKE)
        .option("startingOffsets", "latest")
        .load()
    )
    weather_raw = (
        sp
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_WEATHER)
        .option("startingOffsets", "latest")
        .load()
    )
    bike_bronze = bike_raw.selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_time")
    weather_bronze = weather_raw.selectExpr("CAST(value AS STRING) as json", "timestamp as kafka_time")

    bike_bronze.writeStream.format("delta").option("checkpointLocation", f"{CHECKPOINTS}/bronze_bike") \
        .outputMode("append").start(f"{BRONZE}/bike")
    weather_bronze.writeStream.format("delta").option("checkpointLocation", f"{CHECKPOINTS}/bronze_weather") \
        .outputMode("append").start(f"{BRONZE}/weather")

    # ---- Silver (parsed, deduped, watermark) ----
    bike_silver = (
        bike_bronze
        .select(from_json(col("json"), bike_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .withWatermark("event_time", "15 minutes")
        .dropDuplicates(["station_id","event_time"])
    )
    weather_silver = (
        weather_bronze
        .select(from_json(col("json"), weather_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .withWatermark("event_time", "15 minutes")
        .dropDuplicates(["event_time"])
    )

    bike_silver.writeStream.format("delta").option("checkpointLocation", f"{CHECKPOINTS}/silver_bike") \
        .outputMode("append").start(f"{SILVER}/bike")

    weather_silver.writeStream.format("delta").option("checkpointLocation", f"{CHECKPOINTS}/silver_weather") \
        .outputMode("append").start(f"{SILVER}/weather")

    # ---- Gold (joined aggregates) ----
    joined = (
        bike_silver.join(
            weather_silver,
            expr("""
                 weather_silver.event_time >= bike_silver.event_time - interval 10 minutes AND
                 weather_silver.event_time <= bike_silver.event_time + interval 10 minutes
            """).replace("bike_silver.","").replace("weather_silver.",""),
            "left"
        )
        .withWatermark("event_time", "15 minutes")
    )

    # availability pct per station per 5-min window
    gold = (
        joined
        .groupBy("station_id", window(col("event_time"), "5 minutes"))
        .agg(
            expr("avg(bikes_available) as avg_bikes"),
            expr("avg(docks_available) as avg_docks"),
            expr("avg(temp_c) as avg_temp_c"),
            expr("avg(coalesce(precip_mm,0)) as avg_precip_mm")
        )
        .select(
            col("station_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_bikes","avg_docks","avg_temp_c","avg_precip_mm"
        )
    )

    gold.writeStream.format("delta").option("checkpointLocation", f"{CHECKPOINTS}/gold_station") \
        .outputMode("complete").start(f"{GOLD}/station_availability")

    sp.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
