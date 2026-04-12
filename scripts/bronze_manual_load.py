"""
bronze_manual_load.py
=====================
Proyecto 2 – FHBD | Carga Manual Bronze

Ejecutar ANTES del DAG de Airflow.
Descarga y sube a MinIO (overwrite por diseño en Bronze):

  bronze/posts/2019/posts_2019.parquet   ← manual
  bronze/posts/2020/posts_2020.parquet   ← manual
  bronze/posts/2021/posts_2021.parquet   ← manual
  bronze/users/2019/users_2019.parquet   ← manual
  bronze/users/2020/users_2020.parquet   ← manual
  bronze/votes/2019/votes_2019.parquet   ← manual (para Gold)
  bronze/votes/2020/votes_2020.parquet   ← manual (para Gold)
  bronze/votes/2021/votes_2021.parquet   ← manual (para Gold)
  bronze/badges/all/badges.parquet       ← manual (para Gold)

  bronze/users/2021/users_2021.parquet   ← lo carga el DAG (pipeline ★)

Uso:
  python bronze_manual_load.py
"""

import os
import io
import logging
import boto3
import clickhouse_connect
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://proyecto2-minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

CH_HOST        = os.getenv("CH_HOST",     "play.clickhouse.com")
CH_PORT        = int(os.getenv("CH_PORT", "443"))
CH_USER        = os.getenv("CH_USER",     "play")
CH_PASSWORD    = os.getenv("CH_PASSWORD", "")
CH_DB          = os.getenv("CH_DB",       "stackoverflow")

MAX_ROWS       = int(os.getenv("MAX_ROWS", "50000"))
YEARS          = [2019, 2020, 2021]


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        secure=(CH_PORT == 443),
    )


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' ya existe.")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        log.info(f"Bucket '{bucket}' creado.")


def already_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def upload_df(s3, df, bucket: str, key: str):
    if df.empty:
        log.warning(f"  ⚠️  DataFrame vacío, saltando: {key}")
        return
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    mb = buffer.getbuffer().nbytes / 1024 / 1024
    log.info(f"  ✅ Subido: s3://{bucket}/{key} ({mb:.2f} MB, {len(df):,} filas)")


# ── Loaders por tabla ─────────────────────────────────────────────────────────
def load_posts(ch, s3, year: int):
    key = f"posts/{year}/posts_{year}.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    log.info(f"  Descargando posts {year}...")
    df = ch.query_df(f"""
        SELECT
            Id,
            PostTypeId,
            AcceptedAnswerId,
            CreationDate,
            Score,
            ViewCount,
            OwnerUserId,
            OwnerDisplayName,
            LastActivityDate,
            Title,
            Tags,
            AnswerCount,
            CommentCount,
            FavoriteCount,
            ContentLicense,
            ParentId,
            ClosedDate
        FROM posts
        WHERE toYear(CreationDate) = {year}
        LIMIT {MAX_ROWS}
    """)
    upload_df(s3, df, BRONZE_BUCKET, key)


def load_users(ch, s3, year: int):
    key = f"users/{year}/users_{year}.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    log.info(f"  Descargando users {year}...")
    df = ch.query_df(f"""
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
        WHERE toYear(CreationDate) = {year}
        LIMIT {MAX_ROWS}
    """)
    upload_df(s3, df, BRONZE_BUCKET, key)


def load_votes(ch, s3, year: int):
    key = f"votes/{year}/votes_{year}.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    log.info(f"  Descargando votes {year}...")
    df = ch.query_df(f"""
        SELECT
            Id,
            PostId,
            VoteTypeId,
            CreationDate,
            UserId,
            BountyAmount
        FROM votes
        WHERE toYear(CreationDate) = {year}
        LIMIT {MAX_ROWS}
    """)
    upload_df(s3, df, BRONZE_BUCKET, key)


def load_badges(ch, s3):
    """Badges no tiene filtro por año útil — se carga completo una sola vez."""
    key = "badges/all/badges.parquet"
    if already_exists(s3, BRONZE_BUCKET, key):
        log.info(f"  ⏩ Ya existe: s3://{BRONZE_BUCKET}/{key}")
        return
    log.info("  Descargando badges (completo)...")
    df = ch.query_df(f"""
        SELECT
            Id,
            UserId,
            Name,
            Date,
            Class,
            TagBased
        FROM badges
        LIMIT {MAX_ROWS * 3}
    """)
    upload_df(s3, df, BRONZE_BUCKET, key)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("CARGA MANUAL BRONZE")
    log.info("Tablas: posts (2019-2021), users (2019-2020),")
    log.info("        votes (2019-2021), badges (completo)")
    log.info("=" * 60)

    s3 = get_s3_client()
    ch = get_ch_client()

    ensure_bucket(s3, BRONZE_BUCKET)

    # Posts — 3 años completos (manual)
    log.info("\n📄 Posts...")
    for year in YEARS:
        load_posts(ch, s3, year)

    # Users — solo 2019 y 2020 (2021 lo carga el pipeline)
    log.info("\n👥 Users (2019 y 2020 — 2021 va por pipeline)...")
    for year in [2019, 2020]:
        load_users(ch, s3, year)

    # Votes — 3 años (para tablas Gold)
    log.info("\n🗳️  Votes...")
    for year in YEARS:
        load_votes(ch, s3, year)

    # Badges — completo (para tablas Gold)
    log.info("\n🏅 Badges...")
    load_badges(ch, s3)

    log.info("\n" + "=" * 60)
    log.info("✅ Carga manual Bronze completada.")
    log.info("Estructura en MinIO:")
    log.info("  bronze/posts/2019/, 2020/, 2021/")
    log.info("  bronze/users/2019/, 2020/  (2021 → pipeline ★)")
    log.info("  bronze/votes/2019/, 2020/, 2021/")
    log.info("  bronze/badges/all/")
    log.info("\nAhora ejecuta el DAG en Airflow ▷")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
