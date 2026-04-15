#!/usr/bin/env python3
"""
load_posts_data.py - Generate and upload synthetic posts data to MinIO
Uses string dates to avoid Spark TIMESTAMP(NANOS) compatibility issues
"""
import boto3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from botocore.client import Config
import io

MINIO_ENDPOINT = "http://proyecto2-minio:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin123"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

np.random.seed(42)

print("Generating synthetic posts data (with string dates for Spark compatibility)...")
for year in [2019, 2020, 2021]:
    n_rows = 2000
    start_date = datetime(year, 1, 1)
    
    df = pd.DataFrame({
        'post_id': np.arange(1, n_rows + 1),
        'owner_user_id': np.random.randint(1, 100, n_rows),
        'title': [f'Post {i}' for i in range(1, n_rows + 1)],
        'body': [f'Content {i}' for i in range(1, n_rows + 1)],
        'creation_date': [(start_date + timedelta(days=int(x))).strftime('%Y-%m-%d %H:%M:%S') for x in np.random.rand(n_rows) * 365],
        'score': np.random.randint(0, 100, n_rows),
        'view_count': np.random.randint(0, 5000, n_rows),
        'favorite_count': np.random.randint(0, 100, n_rows),
        'last_activity_date': [(start_date + timedelta(days=int(x))).strftime('%Y-%m-%d %H:%M:%S') for x in np.random.rand(n_rows) * 365],
    })
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='ms')
    buffer.seek(0)
    
    key = f"posts/{year}/posts_{year}.parquet"
    s3.put_object(Bucket="bronze", Key=key, Body=buffer.getvalue())
    print(f"✅ Uploaded: s3://bronze/{key} ({len(df):,} rows)")

print("✅ All posts data loaded successfully!")
