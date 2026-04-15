#!/usr/bin/env bash
# run_manual_setup.sh
# ===================
# Pre-requisitos manuales del Proyecto 2 (correr UNA sola vez antes del DAG):
#   1. Cargar Parquets manuales a Bronze (post_2020/2021, users_2020, votes, badges)
#   2. Construir nessie.silver.post_hist con Spark + Iceberg + Nessie
#
# Uso:
#   bash scripts/run_manual_setup.sh
#
# Requiere: docker compose up con los servicios proyecto2-airflow-worker y
# proyecto2-spark-master ya corriendo.

set -euo pipefail

AIRFLOW_WORKER="${AIRFLOW_WORKER:-proyecto2-airflow-worker}"
SPARK_MASTER_CT="${SPARK_MASTER_CT:-proyecto2-spark-master}"
SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"

ICEBERG_VERSION="1.6.1"
NESSIE_VERSION="0.95.0"
HADOOP_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.262"
SCALA_VERSION="2.12"

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_${SCALA_VERSION}:${ICEBERG_VERSION},\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_${SCALA_VERSION}:${NESSIE_VERSION},\
org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},\
com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_VERSION}"

echo "════════════════════════════════════════════════════════════════"
echo " [1/2] Bronze: cargando Parquets manuales (posts/users/votes/badges)"
echo "════════════════════════════════════════════════════════════════"
docker exec "${AIRFLOW_WORKER}" python /opt/airflow/scripts/bronze_manual_load.py

echo
echo "════════════════════════════════════════════════════════════════"
echo " [2/2] Silver: construyendo nessie.silver.post_hist"
echo "════════════════════════════════════════════════════════════════"
docker exec "${SPARK_MASTER_CT}" /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --packages "${PACKAGES}" \
  --name silver_post_hist_manual \
  /opt/airflow/scripts/silver_post_hist_manual.py

echo
echo "✅ Setup manual completado. Ya puedes lanzar el DAG en Airflow."
