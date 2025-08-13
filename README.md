# City Mobility Pulse

Real-time bike availability and weather analytics on a mini lakehouse built with Spark, Delta Lake, and a simple web frontend. This project demonstrates a full-stack data pipeline from ingestion to visualization.

## One-liner

`make demo` → streams start, data lands in Delta, API & UI come up.

## Architecture

- **Producers** → **Kafka (Redpanda)**
- **Spark Structured Streaming** → **Delta Lake** (MinIO S3)
- **FastAPI** → serves aggregates and API endpoints
- **React** → displays a map and charts of station availability
