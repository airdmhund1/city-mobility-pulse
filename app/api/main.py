# app/api/main.py
import os
from typing import List, Dict, Any, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


# ---------------------- MinIO / Delta helpers ----------------------

def _s3_opts() -> Dict[str, str]:
    endpoint   = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (access_key and secret_key):
        raise HTTPException(status_code=503, detail="Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in API env.")
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_REGION": "us-east-1",
        "AWS_S3_ADDRESSING_STYLE": "path",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _read_delta(
    rel_path: str,
    select_cols: Optional[List[str]] = None,
    limit: int = 100,
    order_by: Optional[str] = None,
    descending: bool = True,
    eq_filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Read a Delta table at s3://lakehouse/<rel_path> with deltalake 0.9.x,
    apply very simple equality filters and return JSON rows.
    """
    try:
        from deltalake import DeltaTable
    except Exception:
        raise HTTPException(status_code=503, detail="deltalake is not installed in the API image.")

    uri = f"s3://lakehouse/{rel_path}"

    # Open & load to pandas (0.9.x doesn't support filters= in to_pandas)
    try:
        dt = DeltaTable(uri, storage_options=_s3_opts())
        df: pd.DataFrame = dt.to_pandas()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed reading {uri}: {e}")

    if df.empty:
        return []

    # pandas-side equality filters
    if eq_filters:
        for k, v in eq_filters.items():
            if v is None:
                continue
            if k in df.columns:
                df = df[df[k] == v]

    if df.empty:
        return []

    # Keep only requested columns that actually exist
    if select_cols:
        keep = [c for c in select_cols if c in df.columns]
        if keep:
            df = df[keep]

    # Order (if the column exists)
    if order_by and order_by in df.columns:
        df = df.sort_values(order_by, ascending=not descending)

    # Limit
    if limit and limit > 0:
        df = df.head(limit)

    # Convert timestamps & numpy types to JSON-friendly
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].astype("datetime64[ns]").astype(str)
        else:
            # Convert nullable/NumPy dtypes safely
            df[c] = df[c].where(pd.notnull(df[c]), None)

    return df.to_dict(orient="records")


# ---------------------- FastAPI app ----------------------

app = FastAPI(title="City Mobility Pulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173", "http://127.0.0.1:5173",
        "http://localhost:5174", "http://127.0.0.1:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------- SILVER endpoints ----------------------

@app.get("/silver/bike")
def silver_bike(
    station_id: Optional[str] = None,
    limit: int = Query(100, ge=1, le=5000),
):
    # Columns your silver/bike_events stream writes (subset-safe)
    cols = [
        "event_time", "dt",
        "station_id", "bike_city",
        "docks_total", "docks_available",
        "bikes_available", "bikes_occupied",
    ]
    return _read_delta(
        "silver/bike_events",
        select_cols=cols,
        limit=limit,
        order_by="event_time",
        descending=True,
        eq_filters={"station_id": station_id},
    )


@app.get("/silver/weather")
def silver_weather(
    station_id: Optional[str] = None,
    limit: int = Query(100, ge=1, le=5000),
):
    cols = [
        "event_time", "dt",
        "station_id", "weather_city",
        "temp_c", "precip_mm", "humidity",
        "wind_kph", "pressure_mb", "condition",
    ]
    return _read_delta(
        "silver/weather_events",
        select_cols=cols,
        limit=limit,
        order_by="event_time",
        descending=True,
        eq_filters={"station_id": station_id},
    )


# ---------------------- GOLD endpoints ----------------------

@app.get("/gold/bike_minute")
def gold_bike_minute(
    station_id: Optional[str] = None,
    limit: int = Query(200, ge=1, le=10000),
):
    # Adjust columns to match your minute table (safe if some are missing)
    cols = ["minute", "station_id", "bike_city", "bikes_available", "bikes_occupied", "docks_total", "dt"]
    return _read_delta(
        "gold/bike_minute",
        select_cols=cols,
        limit=limit,
        order_by="minute",
        descending=True,
        eq_filters={"station_id": station_id},
    )


@app.get("/gold/bike_hourly")
def gold_bike_hourly(
    station_id: Optional[str] = None,
    limit: int = Query(200, ge=1, le=10000),
):
    cols = [
        "window_start", "window_end", "dt",
        "station_id", "bike_city",
        "bikes_occupied_hour",
        "bikes_available_avg_hour",
        "availability_rate_avg_hour",
        "utilization_rate_avg_hour",
        "docks_total",
    ]
    return _read_delta(
        "gold/bike_usage_hourly",
        select_cols=cols,
        limit=limit,
        order_by="window_start",
        descending=True,
        eq_filters={"station_id": station_id},
    )


@app.get("/gold/weather_hourly")
def gold_weather_hourly(
    station_id: Optional[str] = None,
    limit: int = Query(200, ge=1, le=10000),
):
    cols = [
        "window_start", "window_end", "dt",
        "station_id", "weather_city",
        "avg_temp_c_hour", "sum_precip_mm_hour",
        "avg_humidity_hour", "avg_wind_kph_hour", "avg_pressure_mb_hour",
    ]
    return _read_delta(
        "gold/weather_hourly",
        select_cols=cols,
        limit=limit,
        order_by="window_start",
        descending=True,
        eq_filters={"station_id": station_id},
    )