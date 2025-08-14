import os

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

# Redpanda / Kafka
KAFKA_BROKERS   = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC_BIKE      = os.getenv("KAFKA_TOPIC_BIKE", "bike")
TOPIC_WEATHER   = os.getenv("KAFKA_TOPIC_WEATHER", "weather")

# Lakehouse paths
BRONZE = f"s3a://{S3_BUCKET}/bronze"
SILVER = f"s3a://{S3_BUCKET}/silver"
GOLD   = f"s3a://{S3_BUCKET}/gold"

# Spark checkpoints (mounted volume in docker-compose)
CHECKPOINTS = "/opt/spark/checkpoints"

# Strip scheme for Hadoop endpoint and set SSL flag based on scheme
_endpoint_no_scheme = S3_ENDPOINT.replace("http://", "").replace("https://", "")
_ssl_enabled = "false" if S3_ENDPOINT.startswith("http://") else "true"

DELTA_OPTS = {
    # Delta
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # S3A + MinIO
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": _endpoint_no_scheme,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": S3_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": S3_SECRET_KEY,
    "spark.hadoop.fs.s3a.connection.ssl.enabled": _ssl_enabled,

    # Optional: avoid small-file warnings in local dev
    "spark.sql.shuffle.partitions": "1",
}
