"""
silver_votes_badges_manual.py
==============================
Proyecto 2 – FHBD | Capa Silver — Script Manual

Ejecutar ANTES del DAG.
Construye en Silver:
  - nessie.silver.votes_hist   (desde bronze/votes/2019,2020,2021)
  - nessie.silver.badges_hist  (desde bronze/badges/all)

Ambas son necesarias para las tablas Gold.
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
NESSIE_URL     = os.getenv("NESSIE_URL", "http://proyecto2-nessie:19120/api/v1")
NESSIE_BRANCH  = os.getenv("NESSIE_BRANCH", "main")
SILVER_NS      = os.getenv("SILVER_NAMESPACE", "silver")
YEARS          = [2019, 2020, 2021]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_votes_badges_manual")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URL)
        .config("spark.sql.catalog.nessie.ref",          NESSIE_BRANCH)
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse",    "s3a://iceberg/")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_namespace(spark, namespace):
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")
        log.info(f"Namespace 'nessie.{namespace}' listo.")
    except Exception as e:
        log.warning(f"Namespace: {e}")


def create_and_merge(spark, df, full_table: str, merge_key: str, update_cols: list):
    """Crea la tabla si no existe y ejecuta MERGE."""
    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        log.info(f"Tabla '{full_table}' ya existe.")
    except Exception:
        log.info(f"Creando tabla '{full_table}'...")
        df.limit(0).writeTo(full_table) \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()

    df.createOrReplaceTempView("source_data")

    set_clause = ",\n            ".join(
        [f"target.{c} = source.{c}" for c in update_cols]
    )

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING source_data AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET
                {set_clause}
        WHEN NOT MATCHED THEN
            INSERT *
    """
    log.info(f"Ejecutando MERGE INTO {full_table}...")
    spark.sql(merge_sql)
    log.info("MERGE completado.")


# ── votes_hist ────────────────────────────────────────────────────────────────
def build_votes_hist(spark: SparkSession, namespace: str):
    log.info("\n🗳️  Construyendo votes_hist...")
    dfs = []
    for year in YEARS:
        path = f"s3a://{BRONZE_BUCKET}/votes/{year}/votes_{year}.parquet"
        try:
            df = spark.read.parquet(path)
            df = df.withColumn("anio", F.lit(year))
            dfs.append(df)
            log.info(f"  → votes {year}: {df.count():,} filas")
        except Exception as e:
            log.warning(f"  ⚠️  No se pudo leer {path}: {e}")

    if not dfs:
        raise RuntimeError("No se encontraron archivos de votes en Bronze.")

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    df_silver = (
        df_all
        .withColumn("Id",          F.col("Id").cast("long"))
        .withColumn("PostId",      F.col("PostId").cast("long"))
        .withColumn("VoteTypeId",  F.col("VoteTypeId").cast("int"))
        .withColumn("UserId",      F.col("UserId").cast("long"))
        .withColumn("BountyAmount",F.col("BountyAmount").cast("long"))
        .withColumn("fecha_cargue", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                    .cast(TimestampType()))
        .withColumn("fuente", F.lit("clickhouse_stackoverflow"))
        .fillna({"UserId": -1, "BountyAmount": 0})
    )

    full_table = f"nessie.{namespace}.votes_hist"
    create_and_merge(spark, df_silver, full_table, "Id",
                     ["PostId", "VoteTypeId", "UserId", "BountyAmount",
                      "CreationDate", "anio", "fecha_cargue"])

    total = spark.sql(f"SELECT COUNT(*) as c FROM {full_table}").collect()[0]["c"]
    log.info(f"  ✅ votes_hist total filas: {total:,}")


# ── badges_hist ───────────────────────────────────────────────────────────────
def build_badges_hist(spark: SparkSession, namespace: str):
    log.info("\n🏅 Construyendo badges_hist...")
    path = f"s3a://{BRONZE_BUCKET}/badges/all/badges.parquet"
    try:
        df = spark.read.parquet(path)
        log.info(f"  → badges: {df.count():,} filas")
    except Exception as e:
        raise RuntimeError(f"No se pudo leer badges desde Bronze: {e}")

    df_silver = (
        df
        .withColumn("Id",     F.col("Id").cast("long"))
        .withColumn("UserId", F.col("UserId").cast("long"))
        .withColumn("Class",  F.col("Class").cast("int"))
        .withColumn("TagBased", F.col("TagBased").cast("boolean"))
        .withColumn("fecha_cargue", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                    .cast(TimestampType()))
        .withColumn("fuente", F.lit("clickhouse_stackoverflow"))
        .fillna({"Name": "Unknown", "TagBased": False})
    )

    full_table = f"nessie.{namespace}.badges_hist"
    create_and_merge(spark, df_silver, full_table, "Id",
                     ["UserId", "Name", "Date", "Class", "TagBased", "fecha_cargue"])

    total = spark.sql(f"SELECT COUNT(*) as c FROM {full_table}").collect()[0]["c"]
    log.info(f"  ✅ badges_hist total filas: {total:,}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("SILVER MANUAL — votes_hist + badges_hist")
    log.info("=" * 60)

    spark = build_spark_session()
    log.info(f"Spark version: {spark.version}")

    ensure_namespace(spark, SILVER_NS)
    build_votes_hist(spark, SILVER_NS)
    build_badges_hist(spark, SILVER_NS)

    log.info("\n" + "=" * 60)
    log.info("✅ Silver votes_hist y badges_hist completados.")
    log.info("Tablas listas para Gold:")
    log.info(f"  nessie.{SILVER_NS}.votes_hist")
    log.info(f"  nessie.{SILVER_NS}.badges_hist")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
