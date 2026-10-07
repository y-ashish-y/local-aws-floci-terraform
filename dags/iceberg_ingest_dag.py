"""Daily synthetic taxi -> Iceberg via Spark on K8s."""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.apache.spark.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago
import os
import yaml

with open(os.path.join(os.path.dirname(__file__), "spark-application-template.yaml")) as f:
    APP_TEMPLATE = yaml.safe_load(f)

with DAG(
    dag_id="iceberg_taxi_ingest",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["iceberg", "spark", "nessie"],
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_taxi_ingest",
        namespace="data-platform",
        application_file=APP_TEMPLATE,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
    sense = SparkKubernetesSensor(
        task_id="sense_taxi_ingest",
        namespace="data-platform",
        application_name="taxi-ingest-{{ ds_nodash }}",
        kubernetes_conn_id="kubernetes_default",
    )
    submit >> sense
