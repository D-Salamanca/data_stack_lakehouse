"""
gold_agg.py
===========
Proyecto 2 – FHBD | Capa Gold — Task 3 del DAG

Pipeline (★ = ejecutado por el DAG):
  ★ cant_post_x_user_hist  → posts por usuario, año y tipo de post
                             Cruza post_hist + users_hist

Tablas adicionales disponibles para notebooks:
  - vote_stats_per_post    → votos +/- por post
  - top_tags               → ranking de etiquetas
  - user_engagement        → interacciones totales por usuario
  - badges_summary         → resumen de insignias por usuario

Todas usan MERGE/upsert en Iceberg con columna fecha_cargue.
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
NESSIE_URL     = os.getenv("NESSIE_URL",   "http://proyecto2-nessie:19120/api/v1")
NESSIE_BRANCH  = os.getenv("NESSIE_BRANCH", "main")
SILVER_NS      = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NS        = os.getenv("GOLD_NAMESPACE",   "gold")
FECHA_CARGUE   = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold_agg")
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


# ── Helpers ───────────────────────────────────────────────────────────────────
def ensure_namespace(spark, namespace: str):
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")
        log.info(f"Namespace 'nessie.{namespace}' listo.")
    except Exception as e:
        log.warning(f"Namespace: {e}")


def write_gold_merge(spark, df, gold_table: str, merge_key: str):
    """Crea la tabla Gold si no existe y ejecuta MERGE/upsert."""
    full_table = f"nessie.{GOLD_NS}.{gold_table}"

    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        log.info(f"  Tabla '{full_table}' existe → MERGE")
    except Exception:
        log.info(f"  Tabla '{full_table}' no existe → CREATE")
        df.limit(0).writeTo(full_table) \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.metadata.compression-codec", "gzip") \
            .createOrReplace()

    df.createOrReplaceTempView(f"src_{gold_table}")

    cols = [c for c in df.columns if c != merge_key]
    set_clause = ",\n            ".join(
        [f"target.{c} = source.{c}" for c in cols]
    )

    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING src_{gold_table} AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    total = spark.sql(f"SELECT COUNT(*) as c FROM {full_table}").collect()[0]["c"]
    log.info(f"  ✅ {full_table} → {total:,} filas")
    return total


# ── ★ Tabla pipeline: cant_post_x_user_hist ───────────────────────────────────
def build_cant_post_x_user_hist(spark: SparkSession):
    """
    Posts por usuario, año y tipo de post.
    Cruza post_hist + users_hist.
    Tabla principal del pipeline Gold ★
    """
    log.info("\n[★] cant_post_x_user_hist")

    df = spark.sql(f"""
        SELECT
            p.owner_user_id                           AS user_id,
            COALESCE(u.DisplayName, 'Unknown')      AS display_name,
            p.year                                  AS anio,
            COUNT(p.post_id)                             AS cant_posts,
            COALESCE(SUM(p.score), 0)               AS total_score,
            ROUND(AVG(p.score), 2)                  AS avg_score,
            COALESCE(SUM(p.view_count), 0)           AS total_views,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)     AS fecha_cargue
        FROM nessie.{SILVER_NS}.post_hist p
        LEFT JOIN nessie.{SILVER_NS}.users_hist u
            ON p.owner_user_id = u.Id
        WHERE p.owner_user_id IS NOT NULL
        GROUP BY
            p.owner_user_id,
            u.DisplayName,
            p.year
    """)

    # Clave compuesta: user_id + anio
    df = df.withColumn(
        "merge_key",
        F.concat_ws("_",
            F.col("user_id").cast("string"),
            F.col("anio").cast("string"),
        )
    )

    return write_gold_merge(spark, df, "cant_post_x_user_hist", "merge_key")


# ── Tablas adicionales (disponibles para notebooks) ───────────────────────────
def build_vote_stats_per_post(spark: SparkSession):
    """Estadísticas de votos por post. Para notebooks."""
    log.info("\n[extra] vote_stats_per_post")

    df = spark.sql(f"""
        SELECT
            p.Id                                            AS post_id,
            COALESCE(p.Title, '')                          AS title,
            p.PostTypeId                                    AS post_type_id,
            p.OwnerUserId                                   AS owner_user_id,
            p.anio                                          AS anio,
            COUNT(v.Id)                                     AS total_votes,
            SUM(CASE WHEN v.VoteTypeId = 2 THEN 1 ELSE 0 END) AS upvotes,
            SUM(CASE WHEN v.VoteTypeId = 3 THEN 1 ELSE 0 END) AS downvotes,
            SUM(CASE
                WHEN v.VoteTypeId = 2 THEN 1
                WHEN v.VoteTypeId = 3 THEN -1
                ELSE 0 END)                                 AS net_votes,
            COALESCE(p.Score, 0)                            AS score,
            COALESCE(p.ViewCount, 0)                        AS view_count,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)             AS fecha_cargue
        FROM nessie.{SILVER_NS}.post_hist p
        LEFT JOIN nessie.{SILVER_NS}.votes_hist v ON p.Id = v.PostId
        GROUP BY p.Id, p.Title, p.PostTypeId, p.OwnerUserId, p.anio, p.Score, p.ViewCount
    """)

    return write_gold_merge(spark, df, "vote_stats_per_post", "post_id")


def build_top_tags(spark: SparkSession):
    """Ranking de etiquetas más usadas. Para notebooks."""
    log.info("\n[extra] top_tags")

    df = spark.sql(f"""
        SELECT
            trim(tag)                               AS tag,
            anio,
            COUNT(*)                                AS cant_preguntas,
            COALESCE(SUM(Score), 0)                 AS total_score,
            ROUND(AVG(Score), 2)                    AS avg_score,
            COALESCE(SUM(ViewCount), 0)             AS total_views,
            COALESCE(SUM(AnswerCount), 0)           AS total_answers,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)     AS fecha_cargue
        FROM nessie.{SILVER_NS}.post_hist
        LATERAL VIEW explode(
            split(regexp_replace(Tags, '[<>]', ' '), '\\\\s+')
        ) t AS tag
        WHERE PostTypeId = 1
          AND Tags IS NOT NULL
          AND Tags != ''
          AND trim(tag) != ''
        GROUP BY trim(tag), anio
        ORDER BY cant_preguntas DESC
    """)

    df = df.withColumn(
        "merge_key",
        F.concat_ws("_", F.col("tag"), F.col("anio").cast("string"))
    )

    return write_gold_merge(spark, df, "top_tags", "merge_key")


def build_user_engagement(spark: SparkSession):
    """Interacciones totales por usuario. Para notebooks."""
    log.info("\n[extra] user_engagement")

    df = spark.sql(f"""
        SELECT
            u.Id                                        AS user_id,
            COALESCE(u.DisplayName, 'Unknown')          AS display_name,
            COALESCE(u.Reputation, 0)                   AS reputation,
            COALESCE(u.Location, '')                    AS location,
            COUNT(DISTINCT p.Id)                        AS total_posts,
            SUM(CASE WHEN p.PostTypeId = 1 THEN 1 ELSE 0 END) AS total_questions,
            SUM(CASE WHEN p.PostTypeId = 2 THEN 1 ELSE 0 END) AS total_answers,
            COALESCE(SUM(p.Score), 0)                   AS total_score,
            COALESCE(SUM(p.ViewCount), 0)               AS total_views,
            COALESCE(SUM(p.CommentCount), 0)            AS total_comments,
            COUNT(DISTINCT v.Id)                        AS total_votes_received,
            COUNT(DISTINCT b.Id)                        AS total_badges,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)         AS fecha_cargue
        FROM nessie.{SILVER_NS}.users_hist u
        LEFT JOIN nessie.{SILVER_NS}.post_hist p ON u.Id = p.OwnerUserId
        LEFT JOIN nessie.{SILVER_NS}.votes_hist v ON p.Id = v.PostId
        LEFT JOIN nessie.{SILVER_NS}.badges_hist b ON u.Id = b.UserId
        GROUP BY u.Id, u.DisplayName, u.Reputation, u.Location
    """)

    return write_gold_merge(spark, df, "user_engagement", "user_id")


def build_badges_summary(spark: SparkSession):
    """Resumen de insignias por usuario. Para notebooks."""
    log.info("\n[extra] badges_summary")

    df = spark.sql(f"""
        SELECT
            b.UserId                                        AS user_id,
            COALESCE(u.DisplayName, 'Unknown')              AS display_name,
            COALESCE(u.Reputation, 0)                       AS reputation,
            COUNT(b.Id)                                     AS total_badges,
            SUM(CASE WHEN b.Class = 1 THEN 1 ELSE 0 END)   AS gold_badges,
            SUM(CASE WHEN b.Class = 2 THEN 1 ELSE 0 END)   AS silver_badges,
            SUM(CASE WHEN b.Class = 3 THEN 1 ELSE 0 END)   AS bronze_badges,
            SUM(CASE WHEN b.TagBased = true THEN 1 ELSE 0 END) AS tag_based_badges,
            COUNT(DISTINCT b.Name)                          AS unique_badge_types,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)             AS fecha_cargue
        FROM nessie.{SILVER_NS}.badges_hist b
        LEFT JOIN nessie.{SILVER_NS}.users_hist u ON b.UserId = u.Id
        GROUP BY b.UserId, u.DisplayName, u.Reputation
    """)

    return write_gold_merge(spark, df, "badges_summary", "user_id")


# ── Main — solo pipeline ★ ────────────────────────────────────────────────────
def main():
    """
    Ejecutado por el DAG de Airflow.
    Solo genera cant_post_x_user_hist (tabla principal del pipeline).
    Las tablas adicionales se generan desde gold_agg.ipynb.
    """
    log.info("=" * 60)
    log.info("GOLD AGG — cant_post_x_user_hist ★")
    log.info("Fuentes: nessie.silver.post_hist + nessie.silver.users_hist")
    log.info("=" * 60)

    spark = build_spark_session()
    log.info(f"Spark version: {spark.version}")

    ensure_namespace(spark, GOLD_NS)

    total = build_cant_post_x_user_hist(spark)

    log.info("\n" + "=" * 60)
    log.info("✅ Gold completado.")
    log.info(f"   nessie.{GOLD_NS}.cant_post_x_user_hist → {total:,} filas")
    log.info("   Para generar las 4 tablas adicionales:")
    log.info("   → Ejecutar notebooks/gold_agg.ipynb")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
