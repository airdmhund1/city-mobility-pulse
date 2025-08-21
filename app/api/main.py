"""
City Mobility Pulse API.

Lightweight read-only endpoints that fetch rows from Delta tables in MinIO and
return compact JSON responses for the UI. No business logic here—just I/O and
small, safe transforms.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


# ---------------------- MinIO / Delta helpers ----------------------
def _s3_opts() -> Dict[str, str]:
    """Build storage options for deltalake using env vars exposed to the API."""
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (access_key and secret_key):
        raise HTTPException(
            status_code=503,
            detail="Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in API env.",
        )
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
    Read a Delta table at s3://lakehouse/<rel_path> via deltalake 0.9.x,
    apply simple equality filters (in pandas), and return JSON rows.
    """
    try:
        from deltalake import DeltaTable  # type: ignore
    except Exception as err:
        raise HTTPException(
            status_code=503,
            detail="deltalake is not installed in the API image.",
        ) from err

    uri = f"s3://lakehouse/{rel_path}"

    # Load to pandas (deltalake 0.9.x lacks native filters in to_pandas()).
    try:
        dt = DeltaTable(uri, storage_options=_s3_opts())
        df: pd.DataFrame = dt.to_pandas()
    except Exception as err:
        raise HTTPException(status_code=503, detail=f"Failed reading {uri}: {err}") from err

    if df.empty:
        return []

    # Equality filters (pandas-side, safe and simple).
    if eq_filters:
        for k, v in eq_filters.items():
            if v is None:
                continue
            if k in df.columns:
                df = df[df[k] == v]

    if df.empty:
        return []

    # Keep only requested columns that actually exist.
    if select_cols:
        keep = [c for c in select_cols if c in df.columns]
        if keep:
            df = df[keep]

    # Optional ordering.
    if order_by and order_by in df.columns:
        df = df.sort_values(order_by, ascending=not descending)

    # Optional limiting.
    if limit and limit > 0:
        df = df.head(limit)

    # Make timestamps/nullable types JSON-friendly.
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].astype("datetime64[ns]").astype(str)
        else:
            df[c] = df[c].where(pd.notnull(df[c]), None)

    return df.to_dict(orient="records")


# ---------------------- FastAPI app ----------------------
app = FastAPI(title="City Mobility Pulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> Dict[str, str]:
    """Simple liveness probe."""
    return {"status": "ok"}


# ---------------------- SILVER endpoints ----------------------
@app.get("/silver/bike")
def silver_bike(
    station_id: Optional[str] = None,
    limit: int = Query(100, ge=1, le=5000),
) -> List[Dict[str, Any]]:
    """Return recent parsed bike events from the Silver layer."""
    cols = [
        "event_time",
        "dt",
        "station_id",
        "bike_city",
        "docks_total",
        "docks_available",
        "bikes_available",
        "bikes_occupied",
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
) -> List[Dict[str, Any]]:
    """Return recent parsed weather events from the Silver layer."""
    cols = [
        "event_time",
        "dt",
        "station_id",
        "weather_city",
        "temp_c",
        "precip_mm",
        "humidity",
        "wind_kph",
        "pressure_mb",
        "condition",
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
) -> List[Dict[str, Any]]:
    """Return per-minute bike metrics from the Gold layer."""
    cols = [
        "minute",
        "station_id",
        "bike_city",
        "bikes_available",
        "bikes_occupied",
        "docks_total",
        "dt",
    ]
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
) -> List[Dict[str, Any]]:
    """Return per-hour bike metrics from the Gold layer."""
    cols = [
        "window_start",
        "window_end",
        "dt",
        "station_id",
        "bike_city",
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
) -> List[Dict[str, Any]]:
    """Return per-hour weather metrics from the Gold layer."""
    cols = [
        "window_start",
        "window_end",
        "dt",
        "station_id",
        "weather_city",
        "avg_temp_c_hour",
        "sum_precip_mm_hour",
        "avg_humidity_hour",
        "avg_wind_kph_hour",
        "avg_pressure_mb_hour",
    ]
    return _read_delta(
        "gold/weather_hourly",
        select_cols=cols,
        limit=limit,
        order_by="window_start",
        descending=True,
        eq_filters={"station_id": station_id},
    )
