"""
silver_post_hist_manual.py
==========================
Proyecto 2 – FHBD | Capa Silver — Script Manual

Ejecutar ANTES del DAG (o manualmente en caso de fallo).
Lee posts 2019, 2020, 2021 desde Bronze y consolida
en la tabla histórica Iceberg: nessie.silver.post_hist

Uso:
  spark-submit silver_post_hist_manual.py
  o ejecutar desde Jupyter notebook silver_transform.ipynb
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
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET   = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
NESSIE_URL      = os.getenv("NESSIE_URL", "http://proyecto2-nessie:19120/api/v1")
NESSIE_BRANCH   = os.getenv("NESSIE_BRANCH", "main")
SILVER_NS       = os.getenv("SILVER_NAMESPACE", "silver")
YEARS           = [2019, 2020, 2021]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_post_hist_manual")
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
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_posts_bronze(spark: SparkSession):
    dfs = []
    for year in YEARS:
        path = f"s3a://{BRONZE_BUCKET}/posts/{year}/posts_{year}.parquet"
        log.info(f"Leyendo Bronze: {path}")
        try:
            df = spark.read.parquet(path)
            df = df.withColumn("anio", F.lit(year))
            dfs.append(df)
            log.info(f"  → {df.count():,} filas del año {year}")
        except Exception as e:
            log.warning(f"  ⚠️  No se pudo leer {path}: {e}")

    if not dfs:
        raise RuntimeError("No se encontraron archivos de posts en Bronze.")

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)
    return df_all


def add_metadata(df):
    return (
        df
        .withColumn("fecha_cargue", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                    .cast(TimestampType()))
        .withColumn("fuente", F.lit("clickhouse_stackoverflow"))
        .withColumn("Id",           F.col("Id").cast("long"))
        .withColumn("PostTypeId",   F.col("PostTypeId").cast("int"))
        .withColumn("Score",        F.col("Score").cast("long"))
        .withColumn("ViewCount",    F.col("ViewCount").cast("long"))
        .withColumn("OwnerUserId",  F.col("OwnerUserId").cast("long"))
        .withColumn("AnswerCount",  F.col("AnswerCount").cast("long"))
        .withColumn("CommentCount", F.col("CommentCount").cast("long"))
        .withColumn("FavoriteCount",F.col("FavoriteCount").cast("long"))
        .fillna({"Title": "", "Tags": "", "OwnerDisplayName": "Unknown"})
    )


def ensure_namespace(spark, namespace):
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")
        log.info(f"Namespace 'nessie.{namespace}' listo.")
    except Exception as e:
        log.warning(f"Namespace: {e}")


def merge_post_hist(spark: SparkSession, df_new, namespace: str):
    full_table = f"nessie.{namespace}.post_hist"

    # Crear tabla si no existe
    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        log.info(f"Tabla '{full_table}' ya existe.")
    except Exception:
        log.info(f"Creando tabla '{full_table}'...")
        df_new.limit(0).writeTo(full_table) \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()

    df_new.createOrReplaceTempView("posts_updates")

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING posts_updates AS source
        ON target.Id = source.Id
        WHEN MATCHED THEN
            UPDATE SET
                target.Score         = source.Score,
                target.ViewCount     = source.ViewCount,
                target.AnswerCount   = source.AnswerCount,
                target.CommentCount  = source.CommentCount,
                target.FavoriteCount = source.FavoriteCount,
                target.LastActivityDate = source.LastActivityDate,
                target.anio          = source.anio,
                target.fecha_cargue  = source.fecha_cargue,
                target.fuente        = source.fuente
        WHEN NOT MATCHED THEN
            INSERT *
    """

    log.info(f"Ejecutando MERGE INTO {full_table}...")
    spark.sql(merge_sql)
    log.info("MERGE completado.")


def main():
    log.info("=" * 60)
    log.info("SILVER MANUAL — post_hist (2019, 2020, 2021)")
    log.info("=" * 60)

    spark = build_spark_session()

    df_bronze = read_posts_bronze(spark)
    log.info(f"Total filas Bronze posts: {df_bronze.count():,}")

    df_silver = add_metadata(df_bronze)

    ensure_namespace(spark, SILVER_NS)
    merge_post_hist(spark, df_silver, SILVER_NS)

    total = spark.sql(
        f"SELECT COUNT(*) as total FROM nessie.{SILVER_NS}.post_hist"
    ).collect()[0]["total"]

    log.info("-" * 60)
    log.info(f"✅ Silver post_hist — total filas: {total:,}")
    log.info(f"   Tabla: nessie.{SILVER_NS}.post_hist")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
