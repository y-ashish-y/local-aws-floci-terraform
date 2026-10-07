# local_aws_floci_terraform

Local AWS lakehouse on [floci](https://floci.io/) (LocalStack replacement, `:4566`) + Terraform + kind + Airflow + Spark + Iceberg (Nessie).

Stack (all local, no AWS bill): `floci (S3) → kind (K8s) → Nessie (Iceberg REST) + Spark Operator → Airflow (SparkKubernetesOperator) → daily DAG ingests synthetic taxi CSV into Iceberg via Spark`.

## Prereqs

- Docker daemon running, user in `docker` group (`sudo usermod -aG docker $USER`, then re-login)
- Tools: `floci`, `terraform`, `kubectl`, `helm`, `kind` — install with `./scripts/install-tools.sh` (brew)

## Quickstart

```bash
./scripts/setup.sh      # floci + terraform + kind + nessie + spark-operator + airflow
./scripts/teardown.sh   # destroy all
```

Airflow UI: `http://localhost:8080` (admin/admin). Nessie API: `http://localhost:19120`.

DAG `iceberg_taxi_ingest` (@daily): generates synthetic NYC-taxi CSV → `spark-submit` (Iceberg+Nessie packages) → `s3a://lake-warehouse/wh` catalog `nessie.bronze.taxi_trips`.

## Layout

- `terraform/` — S3 buckets (`lake-raw`, `lake-warehouse`) against floci endpoint
- `kind/cluster.yaml` — single-node kind cluster
- `helm/values-airflow.yaml` — Airflow Helm overrides (KubernetesExecutor + spark provider)
- `k8s/` — namespaces, SparkApplication template
- `spark/jobs/ingest_taxi.py` — PySpark → Iceberg/Nessie job
- `spark/Dockerfile` — `apache/spark:3.5.6` + job baked in
- `dags/iceberg_ingest_dag.py` — Airflow DAG (SparkKubernetesOperator + Sensor)
- `scripts/` — install/setup/teardown
