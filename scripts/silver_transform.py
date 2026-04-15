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
from pyspark.sql.types import TimestampType, BinaryType

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


CANONICAL_USER_COLS = {
    # variantes posibles → nombre canónico (snake_case)
    "id": "id", "Id": "id",
    "reputation": "reputation", "Reputation": "reputation",
    "creationdate": "creation_date", "CreationDate": "creation_date",
    "creation_date": "creation_date",
    "displayname": "display_name", "DisplayName": "display_name",
    "display_name": "display_name",
    "lastaccessdate": "last_access_date", "LastAccessDate": "last_access_date",
    "last_access_date": "last_access_date",
    "views": "views", "Views": "views",
    "upvotes": "up_votes", "UpVotes": "up_votes", "up_votes": "up_votes",
    "downvotes": "down_votes", "DownVotes": "down_votes", "down_votes": "down_votes",
    "location": "location", "Location": "location",
    "accountid": "account_id", "AccountId": "account_id", "account_id": "account_id",
    "websiteurl": "website_url", "WebsiteUrl": "website_url", "website_url": "website_url",
    "aboutme": "about_me", "AboutMe": "about_me", "about_me": "about_me",
}

REQUIRED_USER_COLS = [
    "id", "reputation", "creation_date", "display_name", "last_access_date",
    "views", "up_votes", "down_votes", "location", "account_id",
]


CANONICAL_USER_TYPES = {
    "id": "long",
    "reputation": "long",
    "creation_date": "timestamp",
    "display_name": "string",
    "last_access_date": "timestamp",
    "views": "long",
    "up_votes": "long",
    "down_votes": "long",
    "location": "string",
    "account_id": "long",
}


def normalize_user_columns(df):
    """Renombra columnas de un DF de users al schema canónico snake_case y
    castea cada columna a su tipo canónico para que el unionByName no falle
    por tipos heterogéneos entre años. Maneja columnas BINARY (caso de
    bronze_manual_load.py para 2019/2020 que escribió strings/ints como bytes
    raw) decodificándolas como UTF-8 antes del cast final."""
    rename_map = {c: CANONICAL_USER_COLS[c] for c in df.columns if c in CANONICAL_USER_COLS}
    df = df.select([F.col(c).alias(rename_map[c]) for c in rename_map])
    for c, t in CANONICAL_USER_TYPES.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
            continue
        if isinstance(df.schema[c].dataType, BinaryType):
            decoded = F.decode(F.col(c), "utf-8")
            df = df.withColumn(c, decoded if t == "string" else decoded.cast(t))
        else:
            df = df.withColumn(c, F.col(c).cast(t))
    return df.select(*CANONICAL_USER_TYPES.keys())


def read_users_bronze(spark: SparkSession):
    """Lee los 3 años de users desde Bronze, normaliza schema y los une."""
    dfs = []
    for year in YEARS:
        path = f"s3a://{BRONZE_BUCKET}/users/{year}/users_{year}.parquet"
        log.info(f"Leyendo Bronze: {path}")
        try:
            df = spark.read.parquet(path)
            df = normalize_user_columns(df)
            df = df.withColumn("anio", F.lit(year).cast("int"))
            n = df.count()
            log.info(f"  → {n:,} filas del año {year}")
            dfs.append(df)
        except Exception as e:
            # No silenciar: si un año falla queremos saberlo y abortar.
            log.error(f"  ✘ Falló lectura de {path}: {e}", exc_info=True)
            raise

    if not dfs:
        raise RuntimeError("No se encontraron archivos de users en Bronze.")

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    return df_all


def add_metadata(df):
    """Tipa columnas, agrega metadata Silver y limpia nulls."""
    return (
        df
        .withColumn("id",                F.col("id").cast("long"))
        .withColumn("reputation",        F.col("reputation").cast("long"))
        .withColumn("creation_date",     F.col("creation_date").cast(TimestampType()))
        .withColumn("last_access_date",  F.col("last_access_date").cast(TimestampType()))
        .withColumn("views",             F.col("views").cast("long"))
        .withColumn("up_votes",          F.col("up_votes").cast("long"))
        .withColumn("down_votes",        F.col("down_votes").cast("long"))
        .withColumn("account_id",        F.col("account_id").cast("long"))
        .withColumn("display_name",      F.col("display_name").cast("string"))
        .withColumn("location",          F.col("location").cast("string"))
        .withColumn("fecha_cargue",      F.current_timestamp())
        .withColumn("fuente",            F.lit("clickhouse_stackoverflow"))
        .fillna({"display_name": "Unknown", "location": "", "account_id": -1})
    )


def collapse_to_latest_year(df):
    """Conserva una sola fila por id quedándose con la del anio más reciente.
    Necesario porque MERGE en Spark falla si el source tiene varias filas que
    matchean la misma key del target."""
    from pyspark.sql.window import Window
    w = Window.partitionBy("id").orderBy(F.col("anio").desc(),
                                         F.col("last_access_date").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
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
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                target.reputation       = source.reputation,
                target.creation_date    = source.creation_date,
                target.last_access_date = source.last_access_date,
                target.display_name     = source.display_name,
                target.views            = source.views,
                target.up_votes         = source.up_votes,
                target.down_votes       = source.down_votes,
                target.location         = source.location,
                target.account_id       = source.account_id,
                target.anio             = source.anio,
                target.fecha_cargue     = source.fecha_cargue,
                target.fuente           = source.fuente
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

    # 2. Agregar metadata Silver y dejar una sola fila por id (último año gana)
    df_silver = collapse_to_latest_year(add_metadata(df_bronze))
    log.info(f"Filas Silver tras dedupe por id: {df_silver.count():,}")

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
