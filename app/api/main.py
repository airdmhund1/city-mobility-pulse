import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd

# ------------------------
# Environment & constants
# ------------------------
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")   # inside docker network
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Gold table locations (Delta)
GOLD_BIKE_HOURLY = f"s3://{BUCKET}/gold/bike_usage_hourly"
GOLD_BIKE_DAILY  = f"s3://{BUCKET}/gold/bike_usage_daily"
GOLD_WX_HOURLY   = f"s3://{BUCKET}/gold/weather_hourly"
GOLD_WX_DAILY    = f"s3://{BUCKET}/gold/weather_daily"
SILVER_BIKE_PATH = f"s3://{BUCKET}/silver/bike_events"
SILVER_WX_PATH   = f"s3://{BUCKET}/silver/weather_events"



STORAGE_OPTS = {
    # MinIO via AWS-compatible keys
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID or "",
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY or "",
    "AWS_ENDPOINT_URL": ENDPOINT,
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

def _require_creds():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise HTTPException(
            status_code=503,
            detail="S3 credentials not found in environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).",
        )

def _read_delta(
    uri: str,
    columns: Optional[List[str]] = None,
    filters: Optional[List[tuple]] = None,   # e.g. [("dt", ">=", "2025-08-20"), ("station_id","=","S012")]
    limit: Optional[int] = None,
    sort_by: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Read a Delta table into pandas. If the installed deltalake version doesn't
    support server-side filters, we read all rows and filter client-side.
    """
    _require_creds()
    try:
        from deltalake import DeltaTable  # lazy import
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="The 'deltalake' package is not installed in the API image.",
        )

    try:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTS)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not open Delta table {uri}: {e}")

    # Try server-side filtering if supported; otherwise fall back to client-side
    df: pd.DataFrame
    try:
        # Newer deltalake versions may support columns= and/or filters=
        if filters is not None or columns is not None:
            # Try safest path first: call with only columns (widely supported)
            if columns is not None:
                df = dt.to_pandas(columns=columns)  # type: ignore[arg-type]
            else:
                df = dt.to_pandas()  # no columns kw
        else:
            df = dt.to_pandas()
    except TypeError:
        # Very old signature—just load everything
        df = dt.to_pandas()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed reading {uri}: {e}")

    # Client-side filtering (works across all versions)
    if filters:
        mask = pd.Series([True] * len(df))
        ops = {
            "=": lambda a, b: a == b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
        }
        for col_name, op, val in filters:
            if col_name not in df.columns or op not in ops:
                # skip unknown columns/operators rather than erroring out
                continue
            try:
                mask = mask & ops[op](df[col_name], val)
            except Exception:
                # If type mismatch, try coercing to string compare as last resort
                mask = mask & ops[op](df[col_name].astype(str), str(val))
        df = df[mask]

    # Column projection (after filtering to avoid errors on missing cols)
    if columns:
        keep = [c for c in columns if c in df.columns]
        if keep:
            df = df[keep]

    # Sorting & limiting
    if sort_by:
        sort_cols = [c for c in sort_by if c in df.columns]
        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=False, kind="mergesort")
    if limit is not None:
        df = df.head(limit)

    return df.reset_index(drop=True)

def _latest_dt(uri: str) -> Optional[str]:
    """
    Peek the latest partition value for 'dt' from the Delta table's metadata.
    Returns ISO date string or None.
    """
    _require_creds()
    try:
        from deltalake import DeltaTable
    except Exception:
        return None

    try:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTS)
        # pull distinct dt values from the log (fast) — fallback to scanning if needed
        files = dt.files()
        # deduce dt from file paths like .../dt=2025-08-20/part-*.parquet
        dts = []
        for p in files:
            # normalize
            parts = p.split("/")
            for seg in parts:
                if seg.startswith("dt="):
                    dts.append(seg.split("=", 1)[1])
        return max(dts) if dts else None
    except Exception:
        return None
    
def _read_delta_as_df(table_path: str) -> "pd.DataFrame":
    from deltalake import DeltaTable
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not (access_key and secret_key):
        raise HTTPException(status_code=503, detail="S3 credentials missing.")

    storage_options = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_REGION": "us-east-1",
        "AWS_S3_ADDRESSING_STYLE": "path",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    dt = DeltaTable(f"s3://lakehouse/{table_path}", storage_options=storage_options)
    return dt.to_pandas()

def _read_delta_to_pandas(uri: str) -> pd.DataFrame:
    """Open a Delta table and read it to pandas (small-ish samples).
    We keep this simple to be compatible with deltalake 0.9.x.
    """
    try:
        from deltalake import DeltaTable  # lazy import
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Missing dependency: 'deltalake'. Add it to app/api/requirements.txt and rebuild the api image.",
        )

    try:
        dt = DeltaTable(uri, storage_options=STORAGE_OPTS)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Could not open Delta table {uri}: {e}")

    try:
        df = dt.to_pandas()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed reading {uri}: {e}")

    return df


def _filter_time(df: pd.DataFrame, minutes: Optional[int], time_col: str = "event_time") -> pd.DataFrame:
    if minutes is None:
        return df
    if time_col not in df.columns:
        return df
    # parse to datetime (UTC) and filter
    ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return df.loc[ts >= cutoff]


def _limit(df: pd.DataFrame, n: int) -> List[Dict[str, Any]]:
    n = max(1, min(n, 5000))  # simple safety cap
    # prefer newest first if we have a time col
    if "event_time" in df.columns:
        try:
            df = df.sort_values("event_time", ascending=False)
        except Exception:
            pass
    return df.head(n).to_dict(orient="records")




# ------------------------
# FastAPI app
# ------------------------
app = FastAPI(title="City Mobility Pulse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

# ------------------------
# Helpers to assemble filters
# ------------------------
def _common_filters(
    dt_from: Optional[str],
    dt_to: Optional[str],
    station_id: Optional[str],
    city_field: Optional[str],
    city_value: Optional[str],
) -> List[tuple]:
    f: List[tuple] = []
    if dt_from:
        f.append(("dt", ">=", dt_from))
    if dt_to:
        f.append(("dt", "<=", dt_to))
    if station_id:
        f.append(("station_id", "=", station_id))
    if city_field and city_value:
        f.append((city_field, "=", city_value))
    return f

# ------------------------
# Default summary endpoint
# ------------------------
@app.get("/stations/summary")
def stations_summary(
    limit: int = Query(20, ge=1, le=1000),
    station_id: Optional[str] = None,
    city: Optional[str] = None,             # bike_city
    dt_from: Optional[str] = None,          # 'YYYY-MM-DD'
    dt_to: Optional[str] = None,
):
    """
    Small summary from the *bike hourly* gold table.
    Shows most recent window per station (default latest dt).
    """
    uri = GOLD_BIKE_HOURLY

    # default to latest dt if user didn't provide a date range
    if not dt_from and not dt_to:
        last = _latest_dt(uri)
        if last:
            dt_from = last
            dt_to = last

    filters = _common_filters(dt_from, dt_to, station_id, "bike_city", city)

    # read a compact set of columns if they exist
    cols_pref = [
        "station_id", "bike_city",
        "window_start", "window_end",
        "bikes_occupied_hour", "bikes_available_avg_hour",
        "availability_rate_avg_hour", "utilization_rate_avg_hour",
        "docks_total", "dt",
    ]
    df = _read_delta(uri, columns=None, filters=filters, limit=None, sort_by=["dt", "window_end"])

    # only keep known columns (table schema may evolve)
    keep = [c for c in cols_pref if c in df.columns]
    if keep:
        df = df[keep]

    # return top N (already sorted newest-first)
    return df.head(limit).to_dict(orient="records")

# ------------------------
# Gold: Bike Hourly
# ------------------------
@app.get("/gold/bike/hourly")
def gold_bike_hourly(
    limit: int = Query(200, ge=1, le=5000),
    station_id: Optional[str] = None,
    bike_city: Optional[str] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
):
    uri = GOLD_BIKE_HOURLY
    if not dt_from and not dt_to:
        last = _latest_dt(uri)
        if last:
            dt_from = last
            dt_to = last

    filters = _common_filters(dt_from, dt_to, station_id, "bike_city", bike_city)
    df = _read_delta(uri, filters=filters, sort_by=["dt", "window_end"])
    return df.head(limit).to_dict(orient="records")

# ------------------------
# Gold: Bike Daily
# ------------------------
@app.get("/gold/bike/daily")
def gold_bike_daily(
    limit: int = Query(200, ge=1, le=5000),
    station_id: Optional[str] = None,
    bike_city: Optional[str] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
):
    uri = GOLD_BIKE_DAILY
    if not dt_from and not dt_to:
        last = _latest_dt(uri)
        if last:
            dt_from = last
            dt_to = last

    filters = _common_filters(dt_from, dt_to, station_id, "bike_city", bike_city)
    df = _read_delta(uri, filters=filters, sort_by=["dt", "window_end"])
    return df.head(limit).to_dict(orient="records")

# ------------------------
# Gold: Weather Hourly
# ------------------------
@app.get("/gold/weather/hourly")
def gold_weather_hourly(
    limit: int = Query(200, ge=1, le=5000),
    station_id: Optional[str] = None,
    weather_city: Optional[str] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
):
    uri = GOLD_WX_HOURLY
    if not dt_from and not dt_to:
        last = _latest_dt(uri)
        if last:
            dt_from = last
            dt_to = last

    filters = _common_filters(dt_from, dt_to, station_id, "weather_city", weather_city)
    df = _read_delta(uri, filters=filters, sort_by=["dt", "window_end"])
    return df.head(limit).to_dict(orient="records")

# ------------------------
# Gold: Weather Daily
# ------------------------
@app.get("/gold/weather/daily")
def gold_weather_daily(
    limit: int = Query(200, ge=1, le=5000),
    station_id: Optional[str] = None,
    weather_city: Optional[str] = None,
    dt_from: Optional[str] = None,
    dt_to: Optional[str] = None,
):
    uri = GOLD_WX_DAILY
    if not dt_from and not dt_to:
        last = _latest_dt(uri)
        if last:
            dt_from = last
            dt_to = last

    filters = _common_filters(dt_from, dt_to, station_id, "weather_city", weather_city)
    df = _read_delta(uri, filters=filters, sort_by=["dt", "window_end"])
    return df.head(limit).to_dict(orient="records")

# ------------------------
# Quick overview endpoint
# ------------------------
@app.get("/gold/overview")
def gold_overview():
    out: Dict[str, Any] = {}
    for name, uri in {
        "bike_hourly": GOLD_BIKE_HOURLY,
        "bike_daily": GOLD_BIKE_DAILY,
        "weather_hourly": GOLD_WX_HOURLY,
        "weather_daily": GOLD_WX_DAILY,
    }.items():
        try:
            df = _read_delta(uri, limit=5, sort_by=["dt"])
            out[name] = {
                "rows_example": df.to_dict(orient="records"),
                "columns": list(df.columns),
                "latest_dt": df["dt"].max() if "dt" in df.columns and not df.empty else None,
                "count_preview": len(df),
            }
        except HTTPException as e:
            out[name] = {"error": e.detail}
        except Exception as e:
            out[name] = {"error": str(e)}
    return out



@app.get("/stations/minute")
def station_minute(
    station_id: str = Query(..., description="e.g. S011"),
    last_n: int = Query(20, ge=1, le=500, description="How many recent minute rows")
):
    """
    Return near-real-time minute aggregates for a station:
    bikes_available, bikes_occupied, docks_total per 1-minute window.
    """
    try:
        df = _read_delta_as_df("gold/bike_minute")
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

    if df.empty or "station_id" not in df.columns:
        return []

    # Cast times & filter
    df["window_end"] = pd.to_datetime(df.get("window_end", pd.NaT))
    out = (
        df.loc[df["station_id"] == station_id]
          .sort_values("window_end")
          .tail(last_n)
          .to_dict(orient="records")
    )
    return out

@app.get("/silver/bike")
def silver_bike(
    station_id: Optional[str] = Query(None, description="Filter by station_id, e.g. S011"),
    city: Optional[str] = Query(None, alias="bike_city", description="Filter by bike_city"),
    minutes: Optional[int] = Query(10, description="Only events in the last N minutes"),
    limit: int = Query(100, ge=1, le=5000),
):
    """Return recent rows from silver/bike_events."""
    df = _read_delta_to_pandas(SILVER_BIKE_PATH)

    # basic column normalize (in case types drift)
    for c in ("station_id", "bike_city"):
        if c in df.columns:
            df[c] = df[c].astype("string")

    if station_id and "station_id" in df.columns:
        df = df[df["station_id"] == station_id]

    if city and "bike_city" in df.columns:
        df = df[df["bike_city"] == city]

    df = _filter_time(df, minutes, time_col="event_time")

    return _limit(df, limit)


# ---------- Silver: Weather ----------
@app.get("/silver/weather")
def silver_weather(
    station_id: Optional[str] = Query(None, description="Filter by station_id"),
    city: Optional[str] = Query(None, alias="weather_city", description="Filter by weather_city"),
    minutes: Optional[int] = Query(10, description="Only events in the last N minutes"),
    limit: int = Query(100, ge=1, le=5000),
):
    """Return recent rows from silver/weather_events."""
    df = _read_delta_to_pandas(SILVER_WX_PATH)

    for c in ("station_id", "weather_city"):
        if c in df.columns:
            df[c] = df[c].astype("string")

    if station_id and "station_id" in df.columns:
        df = df[df["station_id"] == station_id]

    if city and "weather_city" in df.columns:
        df = df[df["weather_city"] == city]

    df = _filter_time(df, minutes, time_col="event_time")
    return _limit(df, limit)