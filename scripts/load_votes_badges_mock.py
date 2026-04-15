#!/usr/bin/env python3
"""
load_votes_badges_mock.py - Generate and upload synthetic votes/badges data to MinIO
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

print("=" * 60)
print("Generating synthetic votes data...")
print("=" * 60)

# Generate votes for 2019-2021
for year in [2019, 2020, 2021]:
    n_rows = 5000
    start_date = datetime(year, 1, 1)
    
    df = pd.DataFrame({
        'vote_id': np.arange(1, n_rows + 1),
        'post_id': np.random.randint(1, 2000, n_rows),
        'user_id': np.random.randint(1, 100, n_rows),
        'vote_type_id': np.random.choice([1, 2, 3, 4, 5], n_rows),
        'creation_date': [(start_date + timedelta(days=int(x))).strftime('%Y-%m-%d %H:%M:%S') for x in np.random.rand(n_rows) * 365],
    })
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='ms')
    buffer.seek(0)
    
    key = f"votes/{year}/votes_{year}.parquet"
    s3.put_object(Bucket="bronze", Key=key, Body=buffer.getvalue())
    print(f"✅ Uploaded: s3://bronze/{key} ({len(df):,} rows)")

print("\n" + "=" * 60)
print("Generating synthetic badges data...")
print("=" * 60)

# Generate badges data (single file, not yearly)
n_rows = 1000
df_badges = pd.DataFrame({
    'badge_id': np.arange(1, n_rows + 1),
    'user_id': np.random.randint(1, 100, n_rows),
    'badge_name': [f'Badge_{i % 20}' for i in range(1, n_rows + 1)],
    'date': [(datetime(2019, 1, 1) + timedelta(days=int(x))).strftime('%Y-%m-%d %H:%M:%S') for x in np.random.rand(n_rows) * 1095],
})

buffer = io.BytesIO()
df_badges.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='ms')
buffer.seek(0)

key = "badges/badges.parquet"
s3.put_object(Bucket="bronze", Key=key, Body=buffer.getvalue())
print(f"✅ Uploaded: s3://bronze/{key} ({len(df_badges):,} rows)")

print("\n" + "=" * 60)
print("✅ All mock data loaded successfully!")
print("=" * 60)
