"""Daily synthetic taxi -> Iceberg via Spark on K8s."""
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
import pendulum
import os
import yaml

with open(os.path.join(os.path.dirname(__file__), "spark-application-template.yaml")) as f:
    APP_TEMPLATE = yaml.safe_load(f)

with DAG(
    dag_id="iceberg_taxi_ingest",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 7, tz="UTC"),
    catchup=False,
    tags=["iceberg", "spark", "nessie"],
) as dag:
    # SparkKubernetesOperator blocks until the driver terminates and then
    # deletes the SparkApplication, so a follow-up SparkKubernetesSensor would
    # always 404. Failure already propagates from this single task.
    submit = SparkKubernetesOperator(
        task_id="submit_taxi_ingest",
        namespace="data-platform",
        template_spec=APP_TEMPLATE,
        # Without this the operator appends a random suffix to metadata.name.
        random_name_suffix=False,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
