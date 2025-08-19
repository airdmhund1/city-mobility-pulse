import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import pandas as pd

# Optional dependencies — only imported when /stations/summary is called
# so the API can still boot even if deltalake isn't installed during dev.

def _load_gold_from_minio() -> List[Dict[str, Any]]:
    """
    Reads the Gold delta table from MinIO and returns a small JSON summary.
    """
    try:
        from deltalake import DeltaTable  # imported here so the app can start even if lib missing
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail="deltalake package is not installed in the API image. Add `deltalake` to app/api/requirements.txt and rebuild the `api` service."
        )

    # Read MinIO connection from env (populated by docker-compose)
    bucket = "lakehouse"
    table_path = "gold/station_availability_5min"
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not (access_key and secret_key):
        raise HTTPException(
            status_code=503,
            detail="S3 credentials not found in environment (S3_ACCESS_KEY / S3_SECRET_KEY)."
        )

    # DeltaTable expects AWS_* style keys
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": endpoint,             # MinIO endpoint INSIDE docker network
        "AWS_REGION": "us-east-1",
        "AWS_S3_ADDRESSING_STYLE": "path",        # MinIO usually needs path-style
        "AWS_ALLOW_HTTP": "true",                 # allow http (not https)
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",     # safe for local MinIO
    }

    uri = f"s3://{bucket}/{table_path}"

    # Open Delta and read to pandas
    try:
        dt = DeltaTable(uri, storage_options=storage_options)
    except Exception as e:
        # surface the real reason (403 with InvalidAccessKeyId, etc.)
        raise HTTPException(status_code=503, detail=f"Could not open Delta table at {uri}: {e}")

    try:
        df = dt.to_pandas()  # small table → OK. For big tables, filter/select first.
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to read table data: {e}")

    # Convert to a compact JSON (adjust fields to your schema)
    # If your table columns differ, tweak this part:
    cols = [c for c in df.columns if c in ("station_id")] #, "avg_bikes", "avg_docks"
    if not cols:
        # fallback: just show first few rows
        return df.head(20).to_dict(orient="records")

    return df[cols].head(50).to_dict(orient="records")

    # """Read Gold table from MinIO (S3-compatible) using delta-rs.
    # Returns a list of dicts with station_id, avg_bikes, avg_docks.
    # """
    # try:
    #     from deltalake import DeltaTable  # type: ignore
    # except Exception as e:
    #     raise RuntimeError(
    #         "deltalake package is not installed in the API image. Add `deltalake` to app/api/requirements.txt and rebuild the `api` service."
    #     ) from e

    # # Environment-driven config so it works locally and in Compose
    # endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    # bucket = os.getenv("S3_BUCKET", "lakehouse")

    # table_uri = f"s3://{bucket}/gold/station_availability_5min"

    # storage_options = {
    #     # creds
    #     "AWS_ACCESS_KEY_ID": os.getenv("S3_ACCESS_KEY", "minioadmin"),
    #     "AWS_SECRET_ACCESS_KEY": os.getenv("S3_SECRET_KEY", "minioadmin"),
    #     # endpoint + style for MinIO
    #     "AWS_ENDPOINT_URL": endpoint,
    #     "AWS_S3_ADDRESSING_STYLE": "path",
    #     # allow non-TLS local endpoint
    #     "AWS_ALLOW_HTTP": "true",
    #     # region is required by SDK even if not used by MinIO
    #     "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    # }

    # try:
    #     dt = DeltaTable(table_uri, storage_options=storage_options)
    # except Exception as e:
    #     raise RuntimeError(f"Could not open Delta table at {table_uri}: {e}") from e

    # # Read the latest snapshot to pandas
    # try:
    #     # Prefer Arrow -> pandas to avoid dtype surprises
    #     pdf = dt.to_pyarrow_table().to_pandas()
    # except Exception:
    #     pdf = dt.to_pandas()

    # if pdf.empty:
    #     return []

    # cols = set(c.lower() for c in pdf.columns)
    # # If the Gold table already has the aggregated columns, just pick them
    # if {"station_id", "avg_bikes", "avg_docks"}.issubset(cols):
    #     view = pdf[["station_id", "avg_bikes", "avg_docks"]]
    # else:
    #     # Otherwise derive a simple avg summary from raw columns if present
    #     import pandas as pd  # local import
    #     sid = "station_id" if "station_id" in pdf.columns else None
    #     bikes = "bikes" if "bikes" in pdf.columns else None
    #     docks = "docks" if "docks" in pdf.columns else None
    #     if not (sid and bikes and docks):
    #         raise RuntimeError(
    #             f"Gold table columns not recognized. Found columns: {list(pdf.columns)}"
    #         )
    #     view = (
    #         pdf.groupby(sid)[[bikes, docks]]
    #         .mean()
    #         .rename(columns={bikes: "avg_bikes", docks: "avg_docks"})
    #         .reset_index()
    #     )

    # # Round for UI neatness
    # view["avg_bikes"] = view["avg_bikes"].round(1)
    # view["avg_docks"] = view["avg_docks"].round(1)
    # return view.to_dict(orient="records")


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


@app.get("/stations/summary")
def stations_summary():
    """Serve station summary from the Delta Gold table on MinIO.
    Falls back to a friendly error if the table/env is missing.
    """
    try:
        return _load_gold_from_minio()
    except Exception as e:
        # Return a 503 so the frontend can show a helpful message
        raise HTTPException(status_code=503, detail=str(e))
