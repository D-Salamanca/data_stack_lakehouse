#!/usr/bin/env bash
# run_manual_setup.sh
# ===================
# Pre-requisitos manuales del Proyecto 2 (correr UNA sola vez antes del DAG):
#   1. Cargar Parquets manuales a Bronze (posts 2019/2020/2021, users 2019/2020, votes, badges)
#   2. Construir nessie.silver.post_hist con Spark + Iceberg + Nessie
#   3. Construir nessie.silver.votes_hist + nessie.silver.badges_hist
#
# Uso:
#   bash scripts/run_manual_setup.sh
#
# Requiere: docker compose up con los servicios proyecto2-airflow-scheduler y
# proyecto2-spark-master ya corriendo.
#
# Nota: Con LocalExecutor no hay contenedor airflow-worker.
#       El scheduler ejecuta los tasks y tiene spark-submit en /opt/spark/bin/.

set -euo pipefail

# Con LocalExecutor el scheduler es quien tiene Python + spark-submit + scripts
AIRFLOW_SCHEDULER="${AIRFLOW_SCHEDULER:-proyecto2-airflow-scheduler}"
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
echo " [1/3] Bronze: cargando Parquets manuales (posts/users/votes/badges)"
echo "════════════════════════════════════════════════════════════════"
docker exec "${AIRFLOW_SCHEDULER}" python /opt/airflow/scripts/bronze_manual_load.py

echo
echo "════════════════════════════════════════════════════════════════"
echo " [2/3] Silver: construyendo nessie.silver.post_hist"
echo "════════════════════════════════════════════════════════════════"
docker exec "${AIRFLOW_SCHEDULER}" /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --packages "${PACKAGES}" \
  --conf "spark.sql.iceberg.vectorization.enabled=false" \
  --name silver_post_hist_manual \
  /opt/airflow/scripts/silver_post_hist_manual.py

echo
echo "════════════════════════════════════════════════════════════════"
echo " [3/3] Silver: construyendo nessie.silver.votes_hist + badges_hist"
echo "════════════════════════════════════════════════════════════════"
docker exec "${AIRFLOW_SCHEDULER}" /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --packages "${PACKAGES}" \
  --conf "spark.sql.iceberg.vectorization.enabled=false" \
  --name silver_votes_badges_manual \
  /opt/airflow/scripts/silver_votes_badges_manual.py

echo
echo "✅ Setup manual completado (3/3 pasos)."
echo "   Tablas Silver disponibles:"
echo "   - nessie.silver.post_hist"
echo "   - nessie.silver.votes_hist"
echo "   - nessie.silver.badges_hist"
echo "   Ya puedes lanzar el DAG en Airflow."
