"""
bronze_ingest.py
================
Proyecto 2 – FHBD | Capa Bronze — Task 1 del DAG

Usa la librería dlt (Data Load Tool) para ingestar users_2021.

NOTA: La fuente puede ser:
  1. ClickHouse público (si la API está disponible)
  2. Datos sintéticos generados localmente (fallback)

  Destino : s3://bronze/users/2021/users_2021.parquet
  Modo    : OVERWRITE (replace) — comportamiento Bronze
"""

import os
import logging
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import fsspec
from datetime import datetime, timedelta

import dlt

SO_PARQUET_USERS = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet"
HTTP_FS = fsspec.filesystem("https")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración desde variables de entorno ──────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

PIPELINE_YEAR  = 2021
MAX_ROWS       = int(os.getenv("MAX_ROWS", "10000"))

USE_SYNTHETIC = os.getenv("USE_SYNTHETIC", "false").lower() == "true"


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


# ── Fuente dlt: extrae users_2021 (sintéticos por defecto) ─────────────────────
@dlt.resource(
    name="users_2021",
    write_disposition="replace",   # OVERWRITE — comportamiento Bronze
    primary_key="Id",
)
def users_2021_source():
    """
    Resource dlt que genera users para 2021 (datos sintéticos).
    
    Intenta conectar a ClickHouse si está disponible.
    Si no está disponible, usa datos sintéticos.
    
    write_disposition='replace' implementa el override de Bronze.
    """
    if USE_SYNTHETIC:
        log.info("Usando datos SINTÉTICOS para users_2021")
        df = generate_users_2021(n=min(MAX_ROWS, 5000))
        log.info(f"Filas generadas: {len(df):,}")
        yield df
    else:
        log.info(f"Streaming users.parquet de {SO_PARQUET_USERS}")
        f = HTTP_FS.open(SO_PARQUET_USERS, mode="rb", block_size=1024 * 1024)
        pf = pq.ParquetFile(f)

        collected, total = [], 0
        MAX_BATCHES = 500
        for i, batch in enumerate(pf.iter_batches(batch_size=50000)):
            if i >= MAX_BATCHES:
                break
            df = batch.to_pandas()
            df = df[pd.to_datetime(df["CreationDate"], errors="coerce").dt.year == PIPELINE_YEAR]
            if df.empty:
                continue
            remaining = MAX_ROWS - total
            if len(df) > remaining:
                df = df.head(remaining)
            collected.append(df)
            total += len(df)
            if total >= MAX_ROWS:
                break

        if not collected:
            log.warning(f"No se encontraron users con CreationDate año {PIPELINE_YEAR}")
            return
        out = pd.concat(collected, ignore_index=True)
        log.info(f"Filas descargadas users_{PIPELINE_YEAR}: {len(out):,}")
        yield out


# ── Pipeline dlt ──────────────────────────────────────────────────────────────
def build_dlt_pipeline():
    """
    Configura el pipeline dlt apuntando a MinIO (filesystem S3-compatible).
    """
    # Configurar credenciales S3 para dlt vía variables de entorno
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
        # Guardar estado del pipeline en /tmp para no requerir volúmenes extra
        pipelines_dir="/tmp/dlt_pipelines",
    )

    return pipeline


def main():
    log.info("=" * 60)
    log.info("BRONZE INGEST (DLT) — users_%d", PIPELINE_YEAR)
    log.info("Destino: s3://%s/users/%d/", BRONZE_BUCKET, PIPELINE_YEAR)
    log.info("Modo   : OVERWRITE (replace)")
    log.info("=" * 60)

    pipeline = build_dlt_pipeline()

    # Ejecutar el pipeline dlt
    log.info("Ejecutando pipeline dlt...")
    load_info = pipeline.run(users_2021_source())

    log.info("-" * 60)
    log.info("Pipeline dlt completado:")
    log.info(str(load_info))

    # Renombrar el archivo generado por dlt al nombre esperado por Silver
    # dlt genera archivos con UUID, los renombramos a users_2021.parquet
    _rename_dlt_output_to_standard(pipeline)

    log.info("=" * 60)
    log.info("✅ Bronze users_%d ingested exitosamente.", PIPELINE_YEAR)
    log.info("Ruta: s3://%s/users/%d/users_%d.parquet",
             BRONZE_BUCKET, PIPELINE_YEAR, PIPELINE_YEAR)
    log.info("=" * 60)


def _rename_dlt_output_to_standard(pipeline):
    """
    dlt genera archivos con nombres UUID como:
      users_2021/users_2021/load_id.parquet

    Los renombramos al nombre estándar que usan Silver y los notebooks:
      users/2021/users_2021.parquet
    """
    import boto3
    import re
    from botocore.client import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # DLT puede guardar en diferentes estructuras dependiendo de la versión
    # Buscamos en: users/2021/users_2021/ OR users_2021/users_2021/
    prefixes = [
        f"users/{PIPELINE_YEAR}/users_{PIPELINE_YEAR}/",
        f"users_{PIPELINE_YEAR}/users_{PIPELINE_YEAR}/",
    ]
    
    source_key = None
    for prefix in prefixes:
        response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
        
        if "Contents" not in response:
            continue
        
        # Tomar el parquet más reciente generado por dlt
        parquet_files = [
            obj for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ]
        
        if parquet_files:
            source_key = parquet_files[-1]["Key"]
            break
    
    if not source_key:
        log.warning("No se encontraron archivos .parquet en el output de dlt.")
        return

    target_key = f"users/{PIPELINE_YEAR}/users_{PIPELINE_YEAR}.parquet"

    log.info(f"Renombrando: {source_key} → {target_key}")

    # Copiar al path estándar
    s3.copy_object(
        Bucket=BRONZE_BUCKET,
        CopySource={"Bucket": BRONZE_BUCKET, "Key": source_key},
        Key=target_key,
    )

    # Eliminar el original con nombre UUID
    s3.delete_object(Bucket=BRONZE_BUCKET, Key=source_key)
    log.info(f"Archivo estandarizado: s3://{BRONZE_BUCKET}/{target_key}")


if __name__ == "__main__":
    main()
