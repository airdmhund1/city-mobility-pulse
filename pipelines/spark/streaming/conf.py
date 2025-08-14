S3_BASE = "s3a://lakehouse"
BRONZE = f"{S3_BASE}/bronze"
SILVER = f"{S3_BASE}/silver"
GOLD   = f"{S3_BASE}/gold"

KAFKA_BROKERS = "redpanda:9092"
TOPIC_BIKE = "bike_status"
TOPIC_WEATHER = "weather"

CHECKPOINTS = "/opt/spark/checkpoints"

DELTA_OPTS = {
    # Delta + S3A
    "spark.delta.logStore.class": "io.delta.storage.S3SingleDriverLogStore",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

    # MinIO endpoint + auth
    "spark.hadoop.fs.s3a.endpoint": "http://localhost:9001",   # if Spark runs in Docker; use http://localhost:9000 if running locally http://localhost:9001/buckets
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.access.key": "JgBbxZXr2aLlEBHqYftx",
    "spark.hadoop.fs.s3a.secret.key": "wX2grQYZFf1qGRJkZ18WOHcLapbxWHGTt1Z8cMLz",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",  # important when using http
}
