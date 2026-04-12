#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("verify_gold")
    .master("spark://proyecto2-spark-master:7077")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://proyecto2-nessie:19120/api/v1")
    .config("spark.sql.catalog.nessie.warehouse", "s3a://iceberg/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://proyecto2-minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

print("=" * 80)
print("GOLD TABLE: cant_post_x_user_hist")
print("=" * 80)

count_df = spark.sql("SELECT COUNT(*) as total FROM nessie.gold.cant_post_x_user_hist")
total = count_df.collect()[0]["total"]
print(f"\n✅ Total rows in nessie.gold.cant_post_x_user_hist: {total:,}\n")

sample_df = spark.sql("SELECT * FROM nessie.gold.cant_post_x_user_hist LIMIT 10")
print("Sample data (first 10 rows):")
sample_df.show(truncate=False)

spark.stop()
