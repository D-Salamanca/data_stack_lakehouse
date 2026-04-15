"""
bronze_manual_load.py
=====================
Proyecto 2 – FHBD | Carga Manual Bronze

Ejecutar ANTES del DAG de Airflow.

Fuente: dataset público StackOverflow de ClickHouse, publicado como
        parquet en S3 público (sin auth):
        https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/

Destino (MinIO, overwrite por diseño en Bronze):
  bronze/posts/2019/posts_2019.parquet
  bronze/posts/2020/posts_2020.parquet
  bronze/posts/2021/posts_2021.parquet
  bronze/users/2019/users_2019.parquet
  bronze/users/2020/users_2020.parquet
  bronze/votes/2019/votes_2019.parquet
  bronze/votes/2020/votes_2020.parquet
  bronze/votes/2021/votes_2021.parquet
  bronze/badges/all/badges.parquet

  bronze/users/2021/users_2021.parquet   ← lo carga el DAG (pipeline ★)

Uso:
  python bronze_manual_load.py
"""

import io
import logging
import os

import boto3
import aiohttp
import fsspec
import pandas as pd
import pyarrow.parquet as pq
from botocore.client import Config

# Filesystem HTTP con range-reads (NO descarga el parquet completo)
HTTP_FS = fsspec.filesystem(
    "https",
    client_kwargs={"timeout": aiohttp.ClientTimeout(total=600, sock_read=300)},
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://proyecto2-minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

SO_BASE = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet"

MAX_ROWS = int(os.getenv("MAX_ROWS", "50000"))
YEARS = [2019, 2020, 2021]


# ── Helpers ───────────────────────────────────────────────────────────────────
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


def already_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _open_remote_parquet(url: str):
    """Abre un parquet remoto con range-reads (sin descargarlo completo)."""
    log.info(f"  ↓ stream {url}")
    f = HTTP_FS.open(url, mode="rb", block_size=8 * 1024 * 1024)  # 8 MB — menos round-trips en archivos ~3 GB
    return pq.ParquetFile(f)


def download_parquet_head(url: str, n: int) -> pd.DataFrame:
    """Lee las primeras `n` filas de un parquet remoto usando HTTP range-reads."""
    pf = _open_remote_parquet(url)
    collected, total = [], 0
    for batch in pf.iter_batches(batch_size=min(n, 20000)):
        df = batch.to_pandas()
        remaining = n - total
        if len(df) > remaining:
            df = df.head(remaining)
        collected.append(df)
        total += len(df)
        if total >= n:
            break
    return pd.concat(collected, ignore_index=True) if collected else pd.DataFrame()


def download_parquet_filter_year(url: str, year: int, n: int, date_col: str = "CreationDate") -> pd.DataFrame:
    """Filtra por año de `date_col` leyendo el parquet remoto en streaming."""
    log.info(f"  (filtro {date_col} año={year})")
    pf = _open_remote_parquet(url)
    collected, total = [], 0
    # Límite de lectura para no procesar todo el parquet si hay años posteriores
    MAX_BATCHES = 500
    for i, batch in enumerate(pf.iter_batches(batch_size=50000)):
        if i >= MAX_BATCHES:
            break
        df = batch.to_pandas()
        df = df[pd.to_datetime(df[date_col], errors="coerce").dt.year == year]
        if df.empty:
            continue
        remaining = n - total
        if len(df) > remaining:
            df = df.head(remaining)
        collected.append(df)
        total += len(df)
        if total >= n:
            break
    return pd.concat(collected, ignore_index=True) if collected else pd.DataFrame()


def upload_df(s3, df: pd.DataFrame, bucket: str, key: str):
    if df.empty:
        log.warning(f"  ⚠️  DataFrame vacío, saltando: {key}")
        return
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    mb = buffer.getbuffer().nbytes / 1024 / 1024
    log.info(f"  ✅ Subido: s3://{bucket}/{key} ({mb:.2f} MB, {len(df):,} filas)")


# ── Loaders por tabla ─────────────────────────────────────────────────────────
def load_posts(s3, year: int):
    key = f"posts/{year}/posts_{year}.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    url = f"{SO_BASE}/posts/{year}.parquet"
    df = download_parquet_head(url, MAX_ROWS)
    upload_df(s3, df, BRONZE_BUCKET, key)


def load_users(s3, year: int):
    key = f"users/{year}/users_{year}.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    # users.parquet es único; filtramos por año de CreationDate
    url = f"{SO_BASE}/users.parquet"
    df = download_parquet_filter_year(url, year, MAX_ROWS, date_col="CreationDate")
    upload_df(s3, df, BRONZE_BUCKET, key)


def load_votes(s3, year: int):
    key = f"votes/{year}/votes_{year}.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    url = f"{SO_BASE}/votes/{year}.parquet"
    df = download_parquet_head(url, MAX_ROWS)
    upload_df(s3, df, BRONZE_BUCKET, key)


def load_badges(s3):
    key = "badges/all/badges.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    url = f"{SO_BASE}/badges.parquet"
    df = download_parquet_head(url, MAX_ROWS * 3)
    upload_df(s3, df, BRONZE_BUCKET, key)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("CARGA MANUAL BRONZE (desde S3 público de ClickHouse)")
    log.info("Tablas: posts (2019-2021), users (2019-2020),")
    log.info("        votes (2019-2021), badges (completo)")
    log.info("=" * 60)

    s3 = get_s3_client()
    ensure_bucket(s3, BRONZE_BUCKET)

    log.info("\n📄 Posts...")
    for year in YEARS:
        load_posts(s3, year)

    log.info("\n👥 Users (2019 y 2020 — 2021 va por pipeline)...")
    for year in [2019, 2020]:
        load_users(s3, year)

    log.info("\n🗳️  Votes...")
    for year in YEARS:
        load_votes(s3, year)

    log.info("\n🏅 Badges...")
    load_badges(s3)

    log.info("\n" + "=" * 60)
    log.info("✅ Carga manual Bronze completada.")
    log.info("Ahora ejecuta el DAG en Airflow ▷")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
