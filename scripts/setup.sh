#!/usr/bin/env bash
# Full local standup: floci (AWS) -> terraform (S3) -> kind -> Nessie +
# spark-operator + Airflow, then syncs the DAG. Incorporates every fix found
# while debugging (see FIXES.md).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
export PATH="$HOME/.local/bin:/home/linuxbrew/.linuxbrew/bin:$PATH"

echo "== floci =="
floci start
eval "$(floci env)"

echo "== terraform (floci S3) =="
cd "$ROOT/terraform" && terraform init && terraform apply -auto-approve
cd "$ROOT"

echo "== kind =="
kind create cluster --config "$ROOT/kind/cluster.yaml" || true
kubectl wait --for=condition=Ready node --all --timeout=180s

echo "== namespaces + spark RBAC =="
kubectl apply -f "$ROOT/k8s/namespaces.yaml"
kubectl create serviceaccount spark -n data-platform --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding spark-admin --clusterrole=cluster-admin \
  --serviceaccount=data-platform:spark --dry-run=client -o yaml | kubectl apply -f -

echo "== spark image (job baked in) =="
docker build -t lake-spark:3.5-iceberg "$ROOT/spark"
kind load docker-image lake-spark:3.5-iceberg --name lakehouse

echo "== helm repos =="
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
helm repo add nessie https://charts.projectnessie.org/ 2>/dev/null || true
helm repo update

echo "== nessie (Iceberg REST, floci S3 warehouse) =="
kubectl create secret generic nessie-s3-creds -n nessie \
  --from-literal=aws_access_key_id=test --from-literal=aws_secret_access_key=test \
  --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install nessie nessie/nessie -n nessie --create-namespace \
  --version 0.108.8 -f "$ROOT/helm/values-nessie.yaml" \
  --set service.type=NodePort --set service.nodePort=30120

echo "== spark-operator (watch data-platform) =="
helm upgrade --install spark-operator spark-operator/spark-operator \
  -n spark-operator --create-namespace \
  --set 'spark.jobNamespaces={data-platform}'

echo "== airflow =="
kubectl create secret generic airflow-webserver-secret -n data-platform \
  --from-literal=webserver-secret-key="$(openssl rand -hex 16)" \
  --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install airflow apache-airflow/airflow -n data-platform \
  -f "$ROOT/helm/values-airflow.yaml" --version 1.22.0
# Expose the API server on the kind NodePort (chart has no nodePort field).
# kind forwards node 30080 -> host 8080.
kubectl patch svc airflow-api-server -n data-platform \
  -p '{"spec":{"type":"NodePort","ports":[{"name":"api-server","port":8080,"nodePort":30080}]}}' || true
kubectl rollout status deploy/airflow-scheduler -n data-platform --timeout=600s

echo "== sync DAGs (dag-processor pod mounts the shared PVC; scheduler does not) =="
kubectl wait --for=condition=Ready pod -l component=dag-processor \
  -n data-platform --timeout=300s
PROC=$(kubectl get pod -n data-platform -l component=dag-processor \
  -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$ROOT/dags/iceberg_ingest_dag.py" -n data-platform \
  "$PROC:/opt/airflow/dags/iceberg_ingest_dag.py"
kubectl cp "$ROOT/k8s/spark-application-template.yaml" -n data-platform \
  "$PROC:/opt/airflow/dags/spark-application-template.yaml"

echo "DONE. Airflow http://localhost:8080, Nessie http://localhost:19120"
