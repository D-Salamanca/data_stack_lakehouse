"""
silver_transform.py
===================
Proyecto 2 – FHBD | Capa Silver — Task 2 del DAG

Tarea del DAG:
  Lee los 3 años de users desde Bronze (Parquet en MinIO)
  y consolida en una tabla histórica Iceberg usando MERGE (upsert).

  Entrada : s3a://bronze/users/{2019,2020,2021}/users_*.parquet
  Salida  : nessie.silver.users_hist  (Iceberg, merge por Id)

Script manual equivalente para post_hist:
  Ver silver_post_hist_manual.py
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
        .appName("silver_users_hist")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        # Nessie catalog
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",          NESSIE_URL)
        .config("spark.sql.catalog.nessie.ref",          NESSIE_BRANCH)
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse",    "s3a://iceberg/")
        # MinIO S3A
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


def read_users_bronze(spark: SparkSession):
    """Lee los 3 años de users desde Bronze y los une en un solo DataFrame."""
    dfs = []
    for year in YEARS:
        path = f"s3a://{BRONZE_BUCKET}/users/{year}/users_{year}.parquet"
        log.info(f"Leyendo Bronze: {path}")
        try:
            df = spark.read.parquet(path)
            df = df.withColumn("anio", F.lit(year))
            dfs.append(df)
            log.info(f"  → {df.count():,} filas del año {year}")
        except Exception as e:
            log.warning(f"  ⚠️  No se pudo leer {path}: {e}")

    if not dfs:
        raise RuntimeError("No se encontraron archivos de users en Bronze.")

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    return df_all


def add_metadata(df):
    """Agrega columnas de control requeridas por Silver."""
    return (
        df
        .withColumn("fecha_cargue", F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                    .cast(TimestampType()))
        .withColumn("fuente", F.lit("clickhouse_stackoverflow"))
        # Normalizar tipos
        .withColumn("Id",         F.col("Id").cast("long"))
        .withColumn("Reputation", F.col("Reputation").cast("long"))
        .withColumn("Views",      F.col("Views").cast("long"))
        .withColumn("UpVotes",    F.col("UpVotes").cast("long"))
        .withColumn("DownVotes",  F.col("DownVotes").cast("long"))
        .withColumn("AccountId",  F.col("AccountId").cast("long"))
        # Limpiar nulls en strings
        .fillna({"DisplayName": "Unknown", "Location": "", "AccountId": -1})
    )


def ensure_namespace(spark: SparkSession, namespace: str):
    """Crea el namespace en Nessie si no existe."""
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")
        log.info(f"Namespace 'nessie.{namespace}' listo.")
    except Exception as e:
        log.warning(f"Namespace: {e}")


def create_table_if_not_exists(spark: SparkSession, namespace: str, table: str, df):
    """Crea la tabla Iceberg si no existe, inferiendo el schema del DataFrame."""
    full_table = f"nessie.{namespace}.{table}"
    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        log.info(f"Tabla '{full_table}' ya existe.")
    except Exception:
        log.info(f"Creando tabla '{full_table}'...")
        (
            df.limit(0)
            .writeTo(full_table)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.metadata.compression-codec", "gzip")
            .createOrReplace()
        )
        log.info(f"Tabla '{full_table}' creada.")


def merge_users_hist(spark: SparkSession, df_new, namespace: str):
    """
    Hace MERGE (upsert) de los nuevos users en la tabla histórica.
    Clave de merge: Id
    Si existe → actualiza todos los campos
    Si no existe → inserta
    """
    full_table = f"nessie.{namespace}.users_hist"

    # Registrar el DataFrame nuevo como vista temporal
    df_new.createOrReplaceTempView("users_updates")

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING users_updates AS source
        ON target.Id = source.Id
        WHEN MATCHED THEN
            UPDATE SET
                target.Reputation    = source.Reputation,
                target.LastAccessDate = source.LastAccessDate,
                target.DisplayName   = source.DisplayName,
                target.Views         = source.Views,
                target.UpVotes       = source.UpVotes,
                target.DownVotes     = source.DownVotes,
                target.Location      = source.Location,
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
    log.info("SILVER TRANSFORM — users_hist")
    log.info("=" * 60)

    spark = build_spark_session()
    log.info(f"Spark version: {spark.version}")

    # 1. Leer Bronze
    df_bronze = read_users_bronze(spark)
    total_bronze = df_bronze.count()
    log.info(f"Total filas Bronze (3 años): {total_bronze:,}")

    # 2. Agregar metadata Silver
    df_silver = add_metadata(df_bronze)

    # 3. Crear namespace si no existe
    ensure_namespace(spark, SILVER_NS)

    # 4. Crear tabla si no existe
    create_table_if_not_exists(spark, SILVER_NS, "users_hist", df_silver)

    # 5. Merge/upsert
    merge_users_hist(spark, df_silver, SILVER_NS)

    # 6. Verificar resultado
    df_result = spark.sql(f"SELECT COUNT(*) as total FROM nessie.{SILVER_NS}.users_hist")
    total_silver = df_result.collect()[0]["total"]
    log.info("-" * 60)
    log.info(f"✅ Silver users_hist — total filas: {total_silver:,}")
    log.info(f"   Tabla: nessie.{SILVER_NS}.users_hist")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
