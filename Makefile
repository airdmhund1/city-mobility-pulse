ENV_FILE=infra/compose/.env

.PHONY: up down logs rebuild demo spark-stream start-spark wait-topics start-producers

up:
	cd infra/compose && docker compose --env-file .env up -d --build

down:
	cd infra/compose && docker compose --env-file .env down -v

logs:
	cd infra/compose && docker compose --env-file .env logs -f

rebuild:
	cd infra/compose && docker compose --env-file .env build --no-cache

demo: up wait-topics start-producers start-spark

wait-topics:
	sleep 5

start-producers:
	cd infra/compose && docker compose --env-file .env up -d producer-bike producer-weather

spark-stream:
	cd infra/compose && docker compose --env-file .env run --rm spark spark-submit \
	  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
	  --conf spark.hadoop.fs.s3a.access.key=$${S3_ACCESS_KEY} \
	  --conf spark.hadoop.fs.s3a.secret.key=$${S3_SECRET_KEY} \
	  --conf spark.hadoop.fs.s3a.endpoint=$${S3_ENDPOINT} \
	  --conf spark.hadoop.fs.s3a.path.style.access=true \
	  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
	  /opt/spark/workdir/streaming/stream_to_delta.py

start-spark: spark-stream
