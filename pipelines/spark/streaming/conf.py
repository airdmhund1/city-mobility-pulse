S3_BASE = "s3a://lakehouse"
BRONZE = f"{S3_BASE}/bronze"
SILVER = f"{S3_BASE}/silver"
GOLD   = f"{S3_BASE}/gold"

KAFKA_BROKERS = "redpanda:9092"
TOPIC_BIKE = "bike_status"
TOPIC_WEATHER = "weather"

CHECKPOINTS = "/opt/spark/checkpoints"

DELTA_OPTS = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.databricks.delta.optimize.maxFileSize": "134217728"
}
