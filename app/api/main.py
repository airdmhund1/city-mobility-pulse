import os
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import List, Any
import pandas as pd

app = FastAPI(title="City Mobility Pulse API")

S3_ENDPOINT = os.getenv("S3_ENDPOINT","http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET","lakehouse")
DELTA_GOLD = f"{S3_ENDPOINT.replace('http://','s3a://').replace('https://','s3a://')}/{S3_BUCKET}/gold/station_availability"

# For MVP we’ll read via pandas + delta-rs later; keep simple demo endpoint for now.

@app.get("/health")
def health():
    return {"status":"ok"}

@app.get("/stations/summary")
def stations_summary():
    # Placeholder — in week 2 we’ll move Gold -> Postgres for fast serving.
    # For now return mock to unblock UI.
    return [{"station_id":"S001","avg_bikes":8.2,"avg_docks":11.8},
            {"station_id":"S002","avg_bikes":6.9,"avg_docks":13.1}]
