# Local AWS Lakehouse — Terraform + Airflow + Spark + Iceberg

![Terraform](https://img.shields.io/badge/Terraform-7B42BC?logo=terraform&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-0882be)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white)

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

DAG `iceberg_taxi_ingest` (@daily): generates synthetic NYC-taxi rows → Spark job → `nessie.bronze.taxi_trips` in `s3://lake-warehouse/wh`.

Trigger one manually:

```bash
kubectl exec -n data-platform deploy/airflow-scheduler -c scheduler -- \
  airflow dags trigger iceberg_taxi_ingest -r my_run
```

Task logs disappear with the executor pod — capture them while running:

```bash
./scripts/capture-run.sh my_run   # -> /tmp/worker-my_run.log
```

## Layout

- `terraform/` — S3 buckets (`lake-raw`, `lake-warehouse`) against floci endpoint
- `kind/cluster.yaml` — single-node kind cluster (hostPorts 8080, 19120)
- `helm/values-airflow.yaml` — Airflow overrides (KubernetesExecutor, in-cluster conn)
- `helm/values-nessie.yaml` — Nessie Iceberg REST backed by floci S3
- `k8s/` — namespaces, Spark RBAC, api-server NodePort, SparkApplication template
- `spark/jobs/ingest_taxi.py` — PySpark → Iceberg/Nessie job
- `spark/Dockerfile` — `apache/spark:3.5.6` + job baked in
- `dags/iceberg_ingest_dag.py` — Airflow DAG
- `scripts/` — install-tools, setup, teardown, capture-run
- `FIXES.md` — every error hit and its root cause

## Notes

- Needs ~4GB for Docker Desktop (kind + Airflow + Nessie + Spark). Raise
  Settings → Resources → Memory if pods get OOMKilled.
- `setup.sh` is idempotent; re-run it to rebuild after a Docker restart.
- DAG files must be copied into the dag-processor pod (it owns the
  `airflow-dags` PVC); `setup.sh` does this.
