ENV_FILE=infra/compose/.env

.PHONY: up down logs rebuild demo spark-stream start-spark wait-topics start-producers

up:
	cd infra/compose && docker compose --env-file .env up -d --build

down:
	cd infra/compose && docker compose --env-file .env down 

logs:
	cd infra/compose && docker compose --env-file .env logs -f

# api_rebuild:
# 	cd app/api && docker compose --env-file .env build --no-cache fastapi
#   cd app/api && docker compose --env-file .env up -d fastapi 

rebuild:
	cd infra/compose && docker compose --env-file .env build --no-cache

demo: up wait-topics start-producers start-spark

wait-topics:
	sleep 5

build-producers:
	cd infra/compose && docker compose --env-file .env build producer-bike producer-weather

log-producers:
	cd infra/compose && docker compose --env-file .env logs -f producer-bike producer-weather

start-producers:		
	cd infra/compose && docker compose --env-file .env up -d producer-bike producer-weather

spark-stream:
	@set -e; \
	DATE=$$(date +%F); \
	TIME=$$(date +%F_%H-%M-%S); \
	LOG_DIR=logs/$$DATE; \
	LOG_FILE=$$LOG_DIR/spark-stream.$$TIME.log; \
	mkdir -p $$LOG_DIR; \
	echo "Logging to $$LOG_FILE"; \
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
	  /opt/spark/workdir/streaming/stream_to_delta.py \
	  2>&1 | tee ../../$$LOG_FILE

start-spark: spark-stream

# --- UI (Docker) ---
ui-up:
	cd infra/compose && docker compose --env-file .env up -d ui

ui-logs:
	cd infra/compose && docker compose --env-file .env logs -f ui

ui-down:
	cd infra/compose && docker compose --env-file .env stop ui

ui-restart: ui-down ui-up
