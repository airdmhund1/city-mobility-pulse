import os

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
# S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
# S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Redpanda / Kafka
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "redpanda:9092")
TOPIC_BIKE = os.getenv("KAFKA_TOPIC_BIKE", "bike")
TOPIC_WEATHER = os.getenv("KAFKA_TOPIC_WEATHER", "weather")

# Lakehouse paths
BRONZE = f"s3a://{S3_BUCKET}/bronze"
SILVER = f"s3a://{S3_BUCKET}/silver"
GOLD   = f"s3a://{S3_BUCKET}/gold"
CHECKPOINTS = f"s3a://{S3_BUCKET}/checkpoints"


DELTA_OPTS = {
    # Delta
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "io.delta.storage.S3SingleDriverLogStore",


    # S3A + MinIO
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT.replace("http://", "").replace("https://", ""),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if S3_ENDPOINT.startswith("http://") else "true",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID or "",
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY or "",

    "spark.hadoop.fs.s3a.fast.upload": "true",
}
