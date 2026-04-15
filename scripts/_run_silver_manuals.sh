#!/bin/bash
set -e
PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.95.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
SCRIPT="${1:-/opt/airflow/scripts/silver_post_hist_manual.py}"
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.submit.deployMode=client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --packages "$PACKAGES" \
  "$SCRIPT"
