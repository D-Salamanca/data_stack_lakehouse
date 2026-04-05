"""
bronze_ingest.py
================
Proyecto 2 – FHBD | Capa Bronze

Tarea del DAG (Task 1):
  - Descarga users del año 2021 desde ClickHouse público (StackOverflow dataset)
  - Guarda en MinIO como Parquet con OVERWRITE
  - Ruta: s3a://bronze/users/2021/users_2021.parquet

Carga manual previa requerida (antes de ejecutar el DAG):
  - bronze/posts/2020/posts_2020.parquet
  - bronze/posts/2021/posts_2021.parquet
  - bronze/users/2020/users_2020.parquet
"""

import os
import io
import logging
import boto3
import pandas as pd
import clickhouse_connect
from botocore.client import Config
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración desde variables de entorno ──────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET   = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

CH_HOST         = os.getenv("CH_HOST",     "clickhouse.clickhouse.com")
CH_PORT         = int(os.getenv("CH_PORT", "443"))
CH_USER         = os.getenv("CH_USER",     "play")
CH_PASSWORD     = os.getenv("CH_PASSWORD", "")
CH_DB           = os.getenv("CH_DB",       "stackoverflow")

# Año que maneja el pipeline (el resto es carga manual)
PIPELINE_YEAR   = 2021
# Limitar registros para no saturar en clase
MAX_ROWS        = int(os.getenv("MAX_ROWS", "100000"))


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
    """Crea el bucket si no existe."""
    try:
        s3.head_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' ya existe.")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' creado.")


def download_users_from_clickhouse(year: int, max_rows: int) -> pd.DataFrame:
    """
    Descarga usuarios de StackOverflow del año indicado
    desde el ClickHouse público de ClickHouse Cloud.
    """
    log.info(f"Conectando a ClickHouse: {CH_HOST}:{CH_PORT}")

    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        secure=(CH_PORT == 443),
    )

    query = f"""
        SELECT
            Id,
            Reputation,
            CreationDate,
            DisplayName,
            LastAccessDate,
            AboutMe,
            Views,
            UpVotes,
            DownVotes,
            WebsiteUrl,
            Location,
            AccountId
        FROM users
        WHERE toYear(CreationDate) = {year}
        LIMIT {max_rows}
    """

    log.info(f"Descargando users del año {year} (máx {max_rows} filas)...")
    result = client.query_df(query)
    log.info(f"Filas descargadas: {len(result):,}")
    return result


def upload_parquet_to_minio(s3, df: pd.DataFrame, bucket: str, key: str):
    """Sube un DataFrame como Parquet a MinIO (overwrite)."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
    )
    size_mb = buffer.getbuffer().nbytes / 1024 / 1024
    log.info(f"Archivo subido: s3://{bucket}/{key} ({size_mb:.2f} MB)")


def main():
    log.info("=" * 60)
    log.info("BRONZE INGEST — users_%d", PIPELINE_YEAR)
    log.info("=" * 60)

    s3 = get_s3_client()

    # 1. Asegurar que el bucket bronze existe
    ensure_bucket(s3, BRONZE_BUCKET)

    # 2. Descargar users_2021 desde ClickHouse
    df_users = download_users_from_clickhouse(
        year=PIPELINE_YEAR,
        max_rows=MAX_ROWS,
    )

    if df_users.empty:
        raise ValueError(f"No se obtuvieron datos de users para el año {PIPELINE_YEAR}")

    # 3. Subir a MinIO como Parquet (overwrite)
    object_key = f"users/{PIPELINE_YEAR}/users_{PIPELINE_YEAR}.parquet"
    upload_parquet_to_minio(s3, df_users, BRONZE_BUCKET, object_key)

    log.info("-" * 60)
    log.info("Bronze ingest completado exitosamente.")
    log.info("Ruta: s3://%s/%s", BRONZE_BUCKET, object_key)
    log.info("Filas: %d", len(df_users))
    log.info("Columnas: %s", list(df_users.columns))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
