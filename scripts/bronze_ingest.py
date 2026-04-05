"""
bronze_ingest.py
================
Proyecto 2 – FHBD | Capa Bronze — Task 1 del DAG

Usa la librería dlt (Data Load Tool) para ingestar users_2021
desde ClickHouse público (StackOverflow) y guardar en MinIO como Parquet.

  Fuente  : ClickHouse público — stackoverflow.users WHERE year = 2021
  Destino : s3://bronze/users/2021/users_2021.parquet
  Modo    : OVERWRITE (replace) — comportamiento Bronze

dlt se encarga de:
  - Conectar a la fuente
  - Tipar y normalizar los datos
  - Escribir a destino (filesystem S3-compatible)
  - Gestionar el estado del pipeline
"""

import os
import logging

import dlt
import clickhouse_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración desde variables de entorno ──────────────────────────────────
CH_HOST     = os.getenv("CH_HOST",     "clickhouse.clickhouse.com")
CH_PORT     = int(os.getenv("CH_PORT", "443"))
CH_USER     = os.getenv("CH_USER",     "play")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DB       = os.getenv("CH_DB",       "stackoverflow")
MAX_ROWS    = int(os.getenv("MAX_ROWS", "100000"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

PIPELINE_YEAR  = 2021


# ── Fuente dlt: extrae users_2021 desde ClickHouse ───────────────────────────
@dlt.resource(
    name="users_2021",
    write_disposition="replace",   # OVERWRITE — comportamiento Bronze
    primary_key="Id",
)
def users_2021_source():
    """
    Resource dlt que descarga users del año 2021 desde ClickHouse.
    write_disposition='replace' implementa el override de Bronze.
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
            Views,
            UpVotes,
            DownVotes,
            Location,
            AccountId
        FROM users
        WHERE toYear(CreationDate) = {PIPELINE_YEAR}
        LIMIT {MAX_ROWS}
    """

    log.info(f"Descargando users {PIPELINE_YEAR} (máx {MAX_ROWS} filas)...")
    df = client.query_df(query)
    log.info(f"Filas descargadas: {len(df):,}")

    # dlt acepta iterables de dicts o DataFrames
    yield df


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
    log.info("Fuente : ClickHouse público (stackoverflow.users)")
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

    prefix = f"users_{PIPELINE_YEAR}/users_2021/"
    response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        log.warning(f"No se encontraron archivos en s3://{BRONZE_BUCKET}/{prefix}")
        return

    # Tomar el parquet más reciente generado por dlt
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
