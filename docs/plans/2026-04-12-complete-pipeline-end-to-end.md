# Complete Lakehouse Pipeline End-to-End Implementation Plan

> **For Claude:** Use skill: `subagent-driven-development` to execute this plan task-by-task.

**Goal:** Execute the complete Medallion architecture pipeline (Bronze → Silver → Gold) using mock StackOverflow data, verify all layers work end-to-end, configure Dremio, and document the setup.

**Architecture:** 
- Bronze: DLT ingests mock users + manual load of mock posts/votes/badges to MinIO Parquet
- Silver: Spark transforms users → users_hist, posts → post_hist, votes/badges → votes_hist/badges_hist in Iceberg/Nessie
- Gold: Spark aggregates posts-per-user metrics (cant_post_x_user_hist) in Iceberg/Nessie
- Dremio: Connects to Nessie to query all Silver and Gold tables via web UI

**Tech Stack:** Apache Spark, Iceberg, Nessie, MinIO, Airflow, Dremio, PySpark, Python DLT

---

## Task 1: Generate and Load Mock Posts, Votes, Badges to Bronze

**Files:**
- Execute: `scripts/load_posts_data.py` (posts)
- Create: `scripts/load_votes_badges_mock.py` (votes + badges)
- Verify: MinIO buckets via boto3

**Step 1: Generate votes and badges mock data script**

Create `scripts/load_votes_badges_mock.py`:

```python
#!/usr/bin/env python3
"""
load_votes_badges_mock.py - Generate and upload synthetic votes/badges data to MinIO
"""
import boto3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from botocore.client import Config
import io

MINIO_ENDPOINT = "http://proyecto2-minio:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin123"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

np.random.seed(42)

print("=" * 60)
print("Generating synthetic votes data...")
print("=" * 60)

# Generate votes for 2019-2021
for year in [2019, 2020, 2021]:
    n_rows = 5000
    start_date = datetime(year, 1, 1)
    
    df = pd.DataFrame({
        'vote_id': np.arange(1, n_rows + 1),
        'post_id': np.random.randint(1, 2000, n_rows),
        'user_id': np.random.randint(1, 100, n_rows),
        'vote_type_id': np.random.choice([1, 2, 3, 4, 5], n_rows),
        'creation_date': [(start_date + timedelta(days=int(x))).strftime('%Y-%m-%d %H:%M:%S') for x in np.random.rand(n_rows) * 365],
    })
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='ms')
    buffer.seek(0)
    
    key = f"votes/{year}/votes_{year}.parquet"
    s3.put_object(Bucket="bronze", Key=key, Body=buffer.getvalue())
    print(f"✅ Uploaded: s3://bronze/{key} ({len(df):,} rows)")

print("\n" + "=" * 60)
print("Generating synthetic badges data...")
print("=" * 60)

# Generate badges data (single file, not yearly)
n_rows = 1000
df_badges = pd.DataFrame({
    'badge_id': np.arange(1, n_rows + 1),
    'user_id': np.random.randint(1, 100, n_rows),
    'badge_name': [f'Badge_{i % 20}' for i in range(1, n_rows + 1)],
    'date': [(datetime(2019, 1, 1) + timedelta(days=int(x))).strftime('%Y-%m-%d %H:%M:%S') for x in np.random.rand(n_rows) * 1095],
})

buffer = io.BytesIO()
df_badges.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='ms')
buffer.seek(0)

key = "badges/badges.parquet"
s3.put_object(Bucket="bronze", Key=key, Body=buffer.getvalue())
print(f"✅ Uploaded: s3://bronze/{key} ({len(df_badges):,} rows)")

print("\n" + "=" * 60)
print("✅ All mock data loaded successfully!")
print("=" * 60)
```

**Step 2: Run posts data loader**

```bash
cd /opt/airflow/scripts
python load_posts_data.py
```

Expected output:
```
✅ Uploaded: s3://bronze/posts/2019/posts_2019.parquet (2,000 rows)
✅ Uploaded: s3://bronze/posts/2020/posts_2020.parquet (2,000 rows)
✅ Uploaded: s3://bronze/posts/2021/posts_2021.parquet (2,000 rows)
✅ All posts data loaded successfully!
```

**Step 3: Run votes/badges data loader**

```bash
python load_votes_badges_mock.py
```

Expected output:
```
✅ Uploaded: s3://bronze/votes/2019/votes_2019.parquet (5,000 rows)
✅ Uploaded: s3://bronze/votes/2020/votes_2020.parquet (5,000 rows)
✅ Uploaded: s3://bronze/votes/2021/votes_2021.parquet (5,000 rows)
✅ Uploaded: s3://bronze/badges/badges.parquet (1,000 rows)
✅ All mock data loaded successfully!
```

**Step 4: Verify files in MinIO**

```bash
aws s3 --endpoint-url http://localhost:9000 ls s3://bronze/ --recursive
```

Expected: All mock data files visible in output.

---

## Task 2: Create Silver Tables (post_hist, votes_hist, badges_hist)

**Files:**
- Execute: `scripts/silver_post_hist_manual_simple.py`
- Execute: `scripts/silver_votes_badges_manual.py`

**Step 1: Run silver post_hist creation**

```bash
cd /opt/airflow/scripts
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.95.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://proyecto2-nessie:19120/api/v1 \
  --conf spark.sql.catalog.nessie.warehouse=s3a://iceberg/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://proyecto2-minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  silver_post_hist_manual_simple.py
```

Expected: ✅ Table created successfully

**Step 2: Run silver votes/badges creation**

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.95.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://proyecto2-nessie:19120/api/v1 \
  --conf spark.sql.catalog.nessie.warehouse=s3a://iceberg/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://proyecto2-minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  silver_votes_badges_manual.py
```

Expected: ✅ Both tables created successfully

---

## Task 3: Trigger Airflow Pipeline (bronze_ingest → silver_transform → gold_agg)

**Files:**
- Trigger via Airflow: `dags/stackoverflow_pipeline.py`

**Step 1: Trigger full DAG**

```bash
airflow dags trigger stackoverflow_pipeline
```

Or via UI: http://localhost:8080 → DAGs → stackoverflow_pipeline → Trigger DAG

**Step 2: Monitor execution**

Expected: All 3 tasks complete with ✅ SUCCESS

---

## Task 4: Configure Dremio Nessie Data Source

**Files:**
- Access: Dremio UI at `http://localhost:9047`

**Step 1: Login and add Nessie source**

1. Go to http://localhost:9047 → Login (admin/Admin@2026)
2. Admin → Data Sources → +Add Source → Nessie
3. Fill form:
   - Name: `nessie_source`
   - Nessie URL: `http://proyecto2-nessie:19120/api/v2`
   - Reference: `main`
   - Warehouse: `s3a://iceberg/`
   - AWS S3: Endpoint=`http://proyecto2-minio:9000`, Access Key=`minioadmin`, Secret Key=`minioadmin123`
4. Save

**Step 2: Query from Dremio**

Query tab → SQL:
```sql
SELECT * FROM nessie_source.silver.post_hist LIMIT 5;
SELECT COUNT(*) FROM nessie_source.gold.cant_post_x_user_hist;
```

Expected: Results returned from all tables

---

## Task 5: Create Verification Script & Document Setup

**Files:**
- Create: `scripts/verify_pipeline.py`
- Create: `MOCK_DATA_SETUP.md`
- Update: `README.md`

**Step 1: Run verification script**

```bash
spark-submit --master spark://spark-master:7077 scripts/verify_pipeline.py
```

Expected: All Bronze/Silver/Gold checks pass ✅

**Step 2: Create documentation**

Create `MOCK_DATA_SETUP.md` explaining:
- Why mock data is used
- Schema structure
- Setup steps
- Verification commands
- Transition to real data

Update `README.md` with quick-start section for mock data.

---

## Success Criteria

✅ All tasks complete when:
1. All mock data files exist in MinIO (posts, votes, badges)
2. All Silver tables exist in Nessie (users_hist, post_hist, votes_hist, badges_hist)
3. Gold table exists (cant_post_x_user_hist)
4. Airflow DAG run shows ✅ SUCCESS for all 3 tasks
5. Dremio queries return results from all layers
6. Documentation complete and accurate
7. Verification script passes all checks
