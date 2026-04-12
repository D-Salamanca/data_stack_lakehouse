"""
bronze_ingest_mock.py
=====================
Mock version for testing without ClickHouse StackOverflow dataset.

Since the public ClickHouse StackOverflow dataset is not currently available,
this script generates synthetic data matching the expected schema.

The data is used for testing the entire Bronze → Silver → Gold pipeline.
"""

import os
import logging
import pandas as pd
import io
from datetime import datetime, timedelta
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

PIPELINE_YEAR  = 2021
MAX_ROWS       = int(os.getenv("MAX_ROWS", "10000"))


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' ya existe.")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' creado.")


def generate_users(year: int, n: int = 1000) -> pd.DataFrame:
    """Generate synthetic users data."""
    import numpy as np
    np.random.seed(year)
    
    start_date = datetime(year, 1, 1)
    dates = [start_date + timedelta(days=int(x)) for x in np.random.uniform(0, 365, n)]
    
    df = pd.DataFrame({
        'Id': np.arange(1, n + 1),
        'Reputation': np.random.randint(1, 50000, n),
        'CreationDate': dates,
        'DisplayName': [f'User_{i}' for i in range(1, n + 1)],
        'LastAccessDate': [d + timedelta(days=np.random.randint(0, 30)) for d in dates],
        'Views': np.random.randint(0, 10000, n),
        'UpVotes': np.random.randint(0, 1000, n),
        'DownVotes': np.random.randint(0, 100, n),
        'Location': [f'City_{i%50}' for i in range(1, n + 1)],
        'AccountId': np.arange(1, n + 1),
    })
    return df


def generate_posts(year: int, n: int = 2000) -> pd.DataFrame:
    """Generate synthetic posts data."""
    import numpy as np
    np.random.seed(year + 1000)
    
    start_date = datetime(year, 1, 1)
    dates = [start_date + timedelta(days=int(x)) for x in np.random.uniform(0, 365, n)]
    
    df = pd.DataFrame({
        'Id': np.arange(1, n + 1),
        'PostTypeId': np.random.choice([1, 2], n),  # 1=Question, 2=Answer
        'AcceptedAnswerId': np.where(np.random.rand(n) > 0.7, np.random.randint(1, n, n), None),
        'CreationDate': dates,
        'Score': np.random.randint(-5, 100, n),
        'ViewCount': np.random.randint(0, 5000, n),
        'OwnerUserId': np.random.randint(1, 500, n),
        'OwnerDisplayName': [f'User_{i%500}' for i in range(n)],
        'LastActivityDate': [d + timedelta(days=np.random.randint(0, 30)) for d in dates],
        'Title': [f'Post_{i}' for i in range(n)],
        'Tags': ['<python><sql>' if i % 2 == 0 else '<javascript><react>' for i in range(n)],
        'AnswerCount': np.random.randint(0, 10, n),
        'CommentCount': np.random.randint(0, 20, n),
        'FavoriteCount': np.random.randint(0, 50, n),
        'ContentLicense': 'CC BY-SA 3.0',
        'ParentId': None,
        'ClosedDate': None,
    })
    return df


def generate_votes(year: int, n_posts: int = 2000, n: int = 5000) -> pd.DataFrame:
    """Generate synthetic votes data."""
    import numpy as np
    np.random.seed(year + 2000)
    
    start_date = datetime(year, 1, 1)
    dates = [start_date + timedelta(days=int(x)) for x in np.random.uniform(0, 365, n)]
    
    df = pd.DataFrame({
        'Id': np.arange(1, n + 1),
        'PostId': np.random.randint(1, n_posts, n),
        'VoteTypeId': np.random.choice([2, 3], n),  # 2=UpVote, 3=DownVote
        'CreationDate': dates,
        'UserId': np.random.randint(1, 500, n),
        'BountyAmount': np.where(np.random.rand(n) > 0.95, np.random.randint(50, 500, n), None),
    })
    return df


def generate_badges(n: int = 1500) -> pd.DataFrame:
    """Generate synthetic badges data."""
    import numpy as np
    np.random.seed(42)
    
    badge_names = ['Enthusiast', 'Supporter', 'Commentator', 'Critic', 'Citizen Patrol',
                   'Reviewer', 'Custodian', 'Organizer', 'Enlightened', 'Nice Question']
    
    df = pd.DataFrame({
        'Id': np.arange(1, n + 1),
        'UserId': np.random.randint(1, 500, n),
        'Name': [badge_names[i % len(badge_names)] for i in range(n)],
        'Date': [datetime(2021, np.random.randint(1, 13), np.random.randint(1, 29)) for _ in range(n)],
        'Class': np.random.choice([1, 2, 3], n),  # 1=Gold, 2=Silver, 3=Bronze
        'TagBased': [bool(x) for x in np.random.randint(0, 2, n)],
    })
    return df


def upload_df(s3, df, bucket: str, key: str):
    """Upload DataFrame as Parquet to MinIO."""
    if df.empty:
        log.warning(f"  ⚠️  DataFrame vacío, saltando: {key}")
        return
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    mb = buffer.getbuffer().nbytes / 1024 / 1024
    log.info(f"  ✅ Subido: s3://{bucket}/{key} ({mb:.2f} MB, {len(df):,} filas)")


def main():
    log.info("=" * 60)
    log.info("CARGA MANUAL BRONZE (DATOS SINTÉTICOS)")
    log.info("Generando datos de prueba sin ClickHouse")
    log.info("=" * 60)

    s3 = get_s3_client()
    ensure_bucket(s3, BRONZE_BUCKET)

    # Posts — 3 años
    log.info("\n📄 Posts (2019-2021)...")
    for year in [2019, 2020, 2021]:
        key = f"posts/{year}/posts_{year}.parquet"
        df = generate_posts(year, n=min(2000, MAX_ROWS))
        upload_df(s3, df, BRONZE_BUCKET, key)

    # Users — 2019-2020 (2021 va por pipeline)
    log.info("\n👥 Users (2019-2020, 2021 por pipeline)...")
    for year in [2019, 2020]:
        key = f"users/{year}/users_{year}.parquet"
        df = generate_users(year, n=min(1000, MAX_ROWS))
        upload_df(s3, df, BRONZE_BUCKET, key)

    # Votes — 3 años
    log.info("\n🗳️  Votes (2019-2021)...")
    for year in [2019, 2020, 2021]:
        key = f"votes/{year}/votes_{year}.parquet"
        df = generate_votes(year, n_posts=2000, n=min(5000, MAX_ROWS))
        upload_df(s3, df, BRONZE_BUCKET, key)

    # Badges — completo
    log.info("\n🏅 Badges...")
    key = "badges/all/badges.parquet"
    df = generate_badges(n=min(1500, MAX_ROWS))
    upload_df(s3, df, BRONZE_BUCKET, key)

    log.info("\n" + "=" * 60)
    log.info("✅ Carga manual Bronze (SINTÉTICA) completada.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
