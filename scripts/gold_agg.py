"""
gold_agg.py
===========
Proyecto 2 – FHBD | Capa Gold — Task 3 del DAG

Genera 5 tablas Gold desde Silver usando PySpark + Iceberg MERGE:

  1. cant_post_x_user_hist  → posts por usuario por año y tipo
  2. vote_stats_per_post    → votos positivos/negativos por post
  3. top_tags               → ranking de etiquetas más usadas
  4. user_engagement        → interacciones totales por usuario
  5. badges_summary         → resumen de insignias por usuario

Todas escritas en nessie.gold.* con merge/upsert.
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
NESSIE_URL     = os.getenv("NESSIE_URL", "http://proyecto2-nessie:19120/api/v1")
NESSIE_BRANCH  = os.getenv("NESSIE_BRANCH", "main")
SILVER_NS      = os.getenv("SILVER_NAMESPACE", "silver")
GOLD_NS        = os.getenv("GOLD_NAMESPACE", "gold")
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
    """
    Escribe una tabla Gold con MERGE.
    Si no existe la crea; si existe hace upsert por merge_key.
    """
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

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING src_{gold_table} AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET
                {set_clause}
        WHEN NOT MATCHED THEN
            INSERT *
    """
    spark.sql(merge_sql)

    total = spark.sql(f"SELECT COUNT(*) as c FROM {full_table}").collect()[0]["c"]
    log.info(f"  ✅ {full_table} → {total:,} filas")


# ── Tabla 1: cant_post_x_user_hist ────────────────────────────────────────────
def build_cant_post_x_user_hist(spark: SparkSession):
    """
    Cantidad de posts por usuario, año y tipo de post.
    Cruza post_hist + users_hist.
    """
    log.info("\n[1/5] cant_post_x_user_hist")

    df = spark.sql(f"""
        SELECT
            p.OwnerUserId                           AS user_id,
            u.DisplayName                           AS display_name,
            p.anio                                  AS anio,
            p.PostTypeId                            AS post_type_id,
            CASE
                WHEN p.PostTypeId = 1 THEN 'Question'
                WHEN p.PostTypeId = 2 THEN 'Answer'
                ELSE 'Other'
            END                                     AS post_type,
            COUNT(p.Id)                             AS cant_posts,
            SUM(p.Score)                            AS total_score,
            AVG(p.Score)                            AS avg_score,
            SUM(p.ViewCount)                        AS total_views,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)     AS fecha_cargue
        FROM nessie.{SILVER_NS}.post_hist p
        LEFT JOIN nessie.{SILVER_NS}.users_hist u
            ON p.OwnerUserId = u.Id
        WHERE p.OwnerUserId IS NOT NULL
        GROUP BY
            p.OwnerUserId, u.DisplayName,
            p.anio, p.PostTypeId
    """)

    # Clave compuesta como string para el merge
    df = df.withColumn(
        "merge_key",
        F.concat_ws("_",
            F.col("user_id").cast("string"),
            F.col("anio").cast("string"),
            F.col("post_type_id").cast("string")
        )
    )

    write_gold_merge(spark, df, "cant_post_x_user_hist", "merge_key")


# ── Tabla 2: vote_stats_per_post ──────────────────────────────────────────────
def build_vote_stats_per_post(spark: SparkSession):
    """
    Estadísticas de votos por post: positivos, negativos, netos.
    Cruza post_hist + votes_hist.
    """
    log.info("\n[2/5] vote_stats_per_post")

    df = spark.sql(f"""
        SELECT
            p.Id                                    AS post_id,
            p.Title                                 AS title,
            p.PostTypeId                            AS post_type_id,
            p.OwnerUserId                           AS owner_user_id,
            p.anio                                  AS anio,
            COUNT(v.Id)                             AS total_votes,
            SUM(CASE WHEN v.VoteTypeId = 2 THEN 1 ELSE 0 END) AS upvotes,
            SUM(CASE WHEN v.VoteTypeId = 3 THEN 1 ELSE 0 END) AS downvotes,
            SUM(CASE WHEN v.VoteTypeId = 2 THEN 1
                     WHEN v.VoteTypeId = 3 THEN -1
                     ELSE 0 END)                    AS net_votes,
            p.Score                                 AS score,
            p.ViewCount                             AS view_count,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)     AS fecha_cargue
        FROM nessie.{SILVER_NS}.post_hist p
        LEFT JOIN nessie.{SILVER_NS}.votes_hist v
            ON p.Id = v.PostId
        GROUP BY
            p.Id, p.Title, p.PostTypeId,
            p.OwnerUserId, p.anio, p.Score, p.ViewCount
    """)

    write_gold_merge(spark, df, "vote_stats_per_post", "post_id")


# ── Tabla 3: top_tags ─────────────────────────────────────────────────────────
def build_top_tags(spark: SparkSession):
    """
    Ranking de etiquetas más usadas en preguntas.
    Solo usa post_hist (PostTypeId = 1 son preguntas).
    """
    log.info("\n[3/5] top_tags")

    # Explotar el campo Tags (formato: <tag1><tag2>)
    df = spark.sql(f"""
        SELECT
            trim(tag)                               AS tag,
            anio,
            COUNT(*)                                AS cant_preguntas,
            SUM(Score)                              AS total_score,
            AVG(Score)                              AS avg_score,
            SUM(ViewCount)                          AS total_views,
            SUM(AnswerCount)                        AS total_answers,
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

    write_gold_merge(spark, df, "top_tags", "merge_key")


# ── Tabla 4: user_engagement ──────────────────────────────────────────────────
def build_user_engagement(spark: SparkSession):
    """
    Nivel de engagement por usuario: posts + votos recibidos + badges.
    Cruza users_hist + post_hist + votes_hist + badges_hist.
    """
    log.info("\n[4/5] user_engagement")

    df = spark.sql(f"""
        SELECT
            u.Id                                    AS user_id,
            u.DisplayName                           AS display_name,
            u.Reputation                            AS reputation,
            u.Location                              AS location,
            COUNT(DISTINCT p.Id)                    AS total_posts,
            SUM(CASE WHEN p.PostTypeId = 1 THEN 1 ELSE 0 END) AS total_questions,
            SUM(CASE WHEN p.PostTypeId = 2 THEN 1 ELSE 0 END) AS total_answers,
            COALESCE(SUM(p.Score), 0)               AS total_score,
            COALESCE(SUM(p.ViewCount), 0)           AS total_views,
            COALESCE(SUM(p.CommentCount), 0)        AS total_comments,
            COUNT(DISTINCT v.Id)                    AS total_votes_received,
            COUNT(DISTINCT b.Id)                    AS total_badges,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)     AS fecha_cargue
        FROM nessie.{SILVER_NS}.users_hist u
        LEFT JOIN nessie.{SILVER_NS}.post_hist p
            ON u.Id = p.OwnerUserId
        LEFT JOIN nessie.{SILVER_NS}.votes_hist v
            ON p.Id = v.PostId
        LEFT JOIN nessie.{SILVER_NS}.badges_hist b
            ON u.Id = b.UserId
        GROUP BY u.Id, u.DisplayName, u.Reputation, u.Location
    """)

    write_gold_merge(spark, df, "user_engagement", "user_id")


# ── Tabla 5: badges_summary ───────────────────────────────────────────────────
def build_badges_summary(spark: SparkSession):
    """
    Resumen de insignias por usuario: cantidad y tipo.
    Cruza badges_hist + users_hist.
    """
    log.info("\n[5/5] badges_summary")

    df = spark.sql(f"""
        SELECT
            b.UserId                                AS user_id,
            u.DisplayName                           AS display_name,
            u.Reputation                            AS reputation,
            COUNT(b.Id)                             AS total_badges,
            SUM(CASE WHEN b.Class = 1 THEN 1 ELSE 0 END) AS gold_badges,
            SUM(CASE WHEN b.Class = 2 THEN 1 ELSE 0 END) AS silver_badges,
            SUM(CASE WHEN b.Class = 3 THEN 1 ELSE 0 END) AS bronze_badges,
            SUM(CASE WHEN b.TagBased = true THEN 1 ELSE 0 END) AS tag_based_badges,
            COUNT(DISTINCT b.Name)                  AS unique_badge_types,
            CAST('{FECHA_CARGUE}' AS TIMESTAMP)     AS fecha_cargue
        FROM nessie.{SILVER_NS}.badges_hist b
        LEFT JOIN nessie.{SILVER_NS}.users_hist u
            ON b.UserId = u.Id
        GROUP BY b.UserId, u.DisplayName, u.Reputation
    """)

    write_gold_merge(spark, df, "badges_summary", "user_id")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("GOLD AGG — 5 tablas")
    log.info("Fuentes: post_hist, users_hist, votes_hist, badges_hist")
    log.info("=" * 60)

    spark = build_spark_session()
    log.info(f"Spark version: {spark.version}")

    ensure_namespace(spark, GOLD_NS)

    build_cant_post_x_user_hist(spark)
    build_vote_stats_per_post(spark)
    build_top_tags(spark)
    build_user_engagement(spark)
    build_badges_summary(spark)

    log.info("\n" + "=" * 60)
    log.info("✅ Gold completado. Tablas generadas:")
    log.info(f"  nessie.{GOLD_NS}.cant_post_x_user_hist")
    log.info(f"  nessie.{GOLD_NS}.vote_stats_per_post")
    log.info(f"  nessie.{GOLD_NS}.top_tags")
    log.info(f"  nessie.{GOLD_NS}.user_engagement")
    log.info(f"  nessie.{GOLD_NS}.badges_summary")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
