"""
stackoverflow_pipeline.py
==========================
Proyecto 2 – FHBD | DAG principal

Flujo (un solo Play ▷):
  Task 1 → bronze_ingest   : descarga users_2021 con DLT → MinIO Parquet (overwrite)
  Task 2 → silver_transform: consolida users_hist en Iceberg (merge/upsert)
  Task 3 → gold_agg        : genera cant_post_x_user_hist en Iceberg (merge/upsert)

Pre-requisitos (ejecutar manualmente ANTES del DAG):
  1. python scripts/bronze_manual_load.py
       → posts 2020+2021, users_2020, votes, badges en Bronze
  2. spark-submit scripts/silver_post_hist_manual.py
       → nessie.silver.post_hist
  3. spark-submit scripts/silver_votes_badges_manual.py
       → nessie.silver.votes_hist + badges_hist

Conexión requerida en Airflow UI:
  Admin → Connections → (+)
  Conn Id   : spark_default
  Conn Type : Spark
  Host      : spark://spark-master
  Port      : 7077
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── Versiones de JARs ─────────────────────────────────────────────────────────
ICEBERG_VERSION = "1.6.1"
NESSIE_VERSION  = "0.95.0"
HADOOP_VERSION  = "3.3.4"
AWS_SDK_VERSION = "1.12.262"
SCALA_VERSION   = "2.12"

PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_{SCALA_VERSION}:{ICEBERG_VERSION}",
    f"org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_{SCALA_VERSION}:{NESSIE_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
])

# ── Configuración Spark común para Tasks 2 y 3 ───────────────────────────────
SPARK_CONF = {
    "spark.submit.deployMode":  "client",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.executor.instances":         "1",
    "spark.executor.cores":             "1",
    "spark.executor.memory":            "1g",
    "spark.executor.memoryOverhead":    "512m",
    "spark.driver.memory":              "1g",
    "spark.sql.shuffle.partitions":     "4",
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    ),
    "spark.sql.catalog.nessie":                     "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl":        "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri":                 os.getenv("NESSIE_URL", "http://proyecto2-nessie:19120/api/v1"),
    "spark.sql.catalog.nessie.ref":                 os.getenv("NESSIE_BRANCH", "main"),
    "spark.sql.catalog.nessie.authentication.type": "NONE",
    "spark.sql.catalog.nessie.warehouse":           "s3a://iceberg/",
    "spark.hadoop.fs.s3a.endpoint":                 os.getenv("MINIO_ENDPOINT", "http://proyecto2-minio:9000"),
    "spark.hadoop.fs.s3a.access.key":               os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key":               os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    "spark.hadoop.fs.s3a.path.style.access":        "true",
    "spark.hadoop.fs.s3a.impl":                     "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.connection.ssl.enabled":   "false",
}

# ── Environment Variables for Spark Tasks ─────────────────────────────────────
SPARK_ENV_VARS = {
    "MINIO_ENDPOINT":       os.getenv("MINIO_ENDPOINT",       "http://proyecto2-minio:9000"),
    "MINIO_ACCESS_KEY":     os.getenv("MINIO_ACCESS_KEY",     "minioadmin"),
    "MINIO_SECRET_KEY":     os.getenv("MINIO_SECRET_KEY",     "minioadmin123"),
    "MINIO_BUCKET_BRONZE":  os.getenv("MINIO_BUCKET_BRONZE",  "bronze"),
    "MINIO_BUCKET_ICEBERG": os.getenv("MINIO_BUCKET_ICEBERG", "iceberg"),
    "NESSIE_URL":           os.getenv("NESSIE_URL",           "http://proyecto2-nessie:19120/api/v1"),
    "NESSIE_BRANCH":        os.getenv("NESSIE_BRANCH",        "main"),
    "SILVER_NAMESPACE":     os.getenv("SILVER_NAMESPACE",     "silver"),
    "GOLD_NAMESPACE":       os.getenv("GOLD_NAMESPACE",       "gold"),
    "CH_HOST":              os.getenv("CH_HOST",              "play.clickhouse.com"),
    "CH_PORT":              os.getenv("CH_PORT",              "443"),
    "CH_USER":              os.getenv("CH_USER",              "play"),
    "CH_PASSWORD":          os.getenv("CH_PASSWORD",          ""),
    "CH_DB":                os.getenv("CH_DB",                "stackoverflow"),
    "MAX_ROWS":             os.getenv("MAX_ROWS",             "100000"),
}

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "fhbd-grupo",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}


# ── Task 1: Bronze con DLT ────────────────────────────────────────────────────
def bronze_ingest_callable(**context):
    """
    Ejecuta el pipeline dlt para ingestar users_2021 desde ClickHouse a MinIO.
    Corre directamente en el worker de Airflow (no necesita Spark).
    """
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    import bronze_ingest
    bronze_ingest.main()


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="stackoverflow_pipeline",
    description="Pipeline Lakehouse StackOverflow: Bronze(DLT) → Silver(Spark+Iceberg) → Gold(Spark+Iceberg)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["stackoverflow", "bronze", "silver", "gold", "iceberg", "nessie", "dlt"],
    doc_md="""
## Pipeline StackOverflow — Arquitectura Medallion

### Diagrama
```
bronze_ingest (DLT) → silver_transform (Spark) → gold_agg (Spark)
```

### Pre-requisitos (una sola vez antes del primer run)
```bash
# 1. Carga manual Bronze
python scripts/bronze_manual_load.py

# 2. Silver post_hist (manual)
spark-submit scripts/silver_post_hist_manual.py

# 3. Silver votes_hist + badges_hist (manual)
spark-submit scripts/silver_votes_badges_manual.py
```

### Tablas del pipeline ★
| Capa | Tabla | Herramienta |
|---|---|---|
| Bronze | users/2021/users_2021.parquet | DLT + ClickHouse |
| Silver | nessie.silver.users_hist | PySpark + Iceberg merge |
| Gold | nessie.gold.cant_post_x_user_hist | PySpark + Iceberg merge |
    """,
) as dag:

    # ── Task 1: Bronze (DLT) ──────────────────────────────────────────────────
    t_bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest_callable,
        doc_md="""
**Bronze — users_2021 con DLT**
- Usa `dlt` (Data Load Tool) como framework de ingesta
- Fuente: ClickHouse público `stackoverflow.users` WHERE year = 2021
- Destino: `s3://bronze/users/2021/users_2021.parquet`
- Modo: **OVERWRITE** (`write_disposition=replace`)
- DLT gestiona tipado, normalización y estado del pipeline
        """,
    )

    # ── Task 2: Silver (PySpark + Iceberg) ────────────────────────────────────
    def silver_transform_callable(**context):
        """Run silver_transform using spark-submit with proper env vars."""
        import subprocess
        import sys
        
        cmd = [
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
        ]
        
        # Add each config as separate args with proper quoting
        for key, value in SPARK_CONF.items():
            cmd.extend(["--conf", f"{key}={value}"])
        
        # Add packages
        cmd.extend(["--packages", PACKAGES])
        cmd.extend(["--name", "silver_users_hist"])
        cmd.append("--verbose")
        cmd.append("/opt/airflow/scripts/silver_transform.py")
        
        # Execute with env vars
        env = os.environ.copy()
        env.update(SPARK_ENV_VARS)
        
        import logging
        log = logging.getLogger(__name__)
        log.info(f"Running command: {' '.join(cmd)}")
        log.info(f"Environment vars: {', '.join(f'{k}={v}' for k, v in sorted(SPARK_ENV_VARS.items()))}")
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.stdout:
            log.info(f"Spark stdout:\n{result.stdout}")
        if result.stderr:
            log.error(f"Spark stderr:\n{result.stderr}")
        
        if result.returncode != 0:
            raise RuntimeError(f"Spark job failed with return code {result.returncode}")
    
    t_silver = PythonOperator(
        task_id="silver_transform",
        python_callable=silver_transform_callable,
        execution_timeout=timedelta(hours=1),
        doc_md="""
**Silver — users_hist**
- Lee users 2020 + 2021 desde Bronze (Parquet en MinIO)
- Calidad de datos: elimina duplicados, valida nulls, normaliza tipos
- Agrega columna `fecha_cargue`
- Escribe en Iceberg con **MERGE/upsert**: `nessie.silver.users_hist`
        """,
    )

    # ── Task 3: Gold (PySpark + Iceberg) ─────────────────────────────────────
    def gold_agg_callable(**context):
        """Run gold_agg using spark-submit with proper env vars."""
        import subprocess
        import sys
        
        cmd = [
            "/opt/spark/bin/spark-submit",
            "--master", "local[1]",
        ]

        # Override memory-related configs for local mode (driver does the work)
        local_conf = {**SPARK_CONF, "spark.driver.memory": "1500m",
                      "spark.driver.maxResultSize": "512m",
                      "spark.memory.fraction": "0.5",
                      "spark.sql.shuffle.partitions": "2",
                      "spark.sql.iceberg.vectorization.enabled": "false",
                      "spark.driver.extraJavaOptions": "-Xss4m"}
        local_conf.pop("spark.executor.instances", None)
        local_conf.pop("spark.executor.cores", None)
        local_conf.pop("spark.executor.memory", None)
        local_conf.pop("spark.executor.memoryOverhead", None)

        for key, value in local_conf.items():
            cmd.extend(["--conf", f"{key}={value}"])


        # Add packages
        cmd.extend(["--packages", PACKAGES])
        cmd.extend(["--name", "gold_cant_post_x_user_hist"])
        cmd.append("--verbose")
        cmd.append("/opt/airflow/scripts/gold_agg.py")
        
        # Execute with env vars
        env = os.environ.copy()
        env.update(SPARK_ENV_VARS)
        
        import logging
        log = logging.getLogger(__name__)
        log.info(f"Running command: {' '.join(cmd)}")
        log.info(f"Environment vars: {', '.join(f'{k}={v}' for k, v in sorted(SPARK_ENV_VARS.items()))}")
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.stdout:
            log.info(f"Spark stdout:\n{result.stdout}")
        if result.stderr:
            log.error(f"Spark stderr:\n{result.stderr}")
        
        if result.returncode != 0:
            raise RuntimeError(f"Spark job failed with return code {result.returncode}")
    
    t_gold = PythonOperator(
        task_id="gold_agg",
        python_callable=gold_agg_callable,
        execution_timeout=timedelta(hours=2),
        doc_md="""
**Gold — cant_post_x_user_hist**
- Cruza `nessie.silver.post_hist` + `nessie.silver.users_hist`
- Genera métricas: posts por usuario, año y tipo (pregunta/respuesta)
- Columnas: user_id, display_name, anio, post_type, cant_posts,
  total_score, avg_score, total_views, fecha_cargue
- Escribe en Iceberg con **MERGE/upsert**: `nessie.gold.cant_post_x_user_hist`
        """,
    )

    # ── Dependencias ──────────────────────────────────────────────────────────
    t_bronze >> t_silver >> t_gold
