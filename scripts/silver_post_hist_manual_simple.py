#!/usr/bin/env python3
"""
silver_post_hist_manual_simple.py
==================================
Carga manual de posts históricos desde Bronze a Silver (Iceberg/Nessie)
Versión simplificada y compatible con datos sintéticos
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Configuración
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://proyecto2-minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
NESSIE_URL = os.getenv("NESSIE_URL", "http://proyecto2-nessie:19120/api/v1")
YEARS = [2019, 2020, 2021]

def build_spark_session():
    return (
        SparkSession.builder
        .appName("silver_post_hist_manual_simple")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URL)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://iceberg/")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def main():
    log.info("=" * 60)
    log.info("SILVER POST_HIST MANUAL - Simple Version")
    log.info("=" * 60)
    
    spark = build_spark_session()
    
    # Leer datos de Bronze para todos los años
    dfs = []
    for year in YEARS:
        path = f"s3a://bronze/posts/{year}/posts_{year}.parquet"
        try:
            df = spark.read.parquet(path)
            df = df.withColumn("year", F.lit(year))
            dfs.append(df)
            log.info(f"✅ Read {path}: {df.count():,} rows")
        except Exception as e:
            log.warning(f"⚠️ Failed to read {path}: {str(e)}")
    
    if not dfs:
        log.error("❌ No data loaded from Bronze")
        return
    
    # Union de todos los años
    df_union = dfs[0]
    for df in dfs[1:]:
        df_union = df_union.union(df)
    
    # Agregar metadata
    df_silver = (
        df_union
        .withColumn("load_date", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .withColumn("source_system", F.lit("stackoverflow_bronze"))
    )
    
    log.info(f"\n✅ Total rows to write: {df_silver.count():,}")
    log.info(f"Schema: {df_silver.printSchema()}")
    
    # Write to Iceberg using MERGE (upsert)
    try:
        # Crear tabla si no existe
        df_silver.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("nessie.silver.post_hist")
        log.info("✅ Table nessie.silver.post_hist created/updated successfully!")
    except Exception as e:
        log.error(f"❌ Error writing to Iceberg: {str(e)}")
        raise

if __name__ == "__main__":
    main()
