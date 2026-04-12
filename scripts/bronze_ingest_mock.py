"""
bronze_ingest_mock.py
=====================
Mock version for users_2021 ingestion (DLT replacement).

Since the public ClickHouse StackOverflow dataset is not available,
this generates synthetic users for 2021 and uses DLT to write to MinIO.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import dlt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

PIPELINE_YEAR  = 2021
MAX_ROWS       = int(os.getenv("MAX_ROWS", "10000"))


def generate_users_2021(n: int = 5000) -> pd.DataFrame:
    """Generate synthetic users for 2021."""
    np.random.seed(2021)
    
    start_date = datetime(2021, 1, 1)
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


# ── Fuente dlt ────────────────────────────────────────────────────────────────
@dlt.resource(
    name="users_2021",
    write_disposition="replace",
    primary_key="Id",
)
def users_2021_source():
    """Resource dlt que genera users sintéticos para 2021."""
    log.info(f"Generando usuarios sintéticos para {PIPELINE_YEAR}")
    df = generate_users_2021(n=min(MAX_ROWS, 5000))
    log.info(f"Filas generadas: {len(df):,}")
    yield df


# ── Pipeline dlt ──────────────────────────────────────────────────────────────
def build_dlt_pipeline():
    """Configura el pipeline dlt para MinIO."""
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = (
        f"s3://{BRONZE_BUCKET}/users/{PIPELINE_YEAR}"
    )
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL"]          = MINIO_ENDPOINT
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME"]           = "us-east-1"

    pipeline = dlt.pipeline(
        pipeline_name="bronze_users_2021",
        destination="filesystem",
        dataset_name=f"users_{PIPELINE_YEAR}",
        pipelines_dir="/tmp/dlt_pipelines",
    )

    return pipeline


def main():
    log.info("=" * 60)
    log.info("BRONZE INGEST (DLT) — users_%d (SINTÉTICO)", PIPELINE_YEAR)
    log.info("Destino: s3://%s/users/%d/", BRONZE_BUCKET, PIPELINE_YEAR)
    log.info("Modo   : OVERWRITE (replace)")
    log.info("=" * 60)

    pipeline = build_dlt_pipeline()

    log.info("Ejecutando pipeline dlt...")
    load_info = pipeline.run(users_2021_source())

    log.info("-" * 60)
    log.info("Pipeline dlt completado:")
    log.info(str(load_info))

    _rename_dlt_output_to_standard(pipeline)

    log.info("=" * 60)
    log.info("✅ Bronze users_%d ingested exitosamente.", PIPELINE_YEAR)
    log.info("Ruta: s3://%s/users/%d/users_%d.parquet",
             BRONZE_BUCKET, PIPELINE_YEAR, PIPELINE_YEAR)
    log.info("=" * 60)


def _rename_dlt_output_to_standard(pipeline):
    """Rename dlt output to standard naming."""
    import boto3
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    prefix = f"users_{PIPELINE_YEAR}/users_2021/"
    response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        log.warning(f"No se encontraron archivos en s3://{BRONZE_BUCKET}/{prefix}")
        return

    parquet_files = [
        obj for obj in response["Contents"]
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        log.warning("No se encontraron archivos .parquet en el output de dlt.")
        return

    source_key = parquet_files[-1]["Key"]
    target_key = f"users/{PIPELINE_YEAR}/users_{PIPELINE_YEAR}.parquet"

    log.info(f"Renombrando: {source_key} → {target_key}")

    s3.copy_object(
        Bucket=BRONZE_BUCKET,
        CopySource={"Bucket": BRONZE_BUCKET, "Key": source_key},
        Key=target_key,
    )

    s3.delete_object(Bucket=BRONZE_BUCKET, Key=source_key)
    log.info(f"Archivo estandarizado: s3://{BRONZE_BUCKET}/{target_key}")


if __name__ == "__main__":
    main()
