#!/usr/bin/env bash
# Debug helper: trigger a DAG run, capture the executor pod's log AND its
# describe/status before the KubernetesExecutor deletes the pod.
set -uo pipefail
export PATH="$HOME/.local/bin:/home/linuxbrew/.linuxbrew/bin:$PATH"
RUN_ID=${1:-dbg_run_$RANDOM}
OUT=/tmp/worker-$RUN_ID.log
: > "$OUT"

SCHED=$(kubectl get pod -n data-platform -l component=scheduler \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n data-platform "$SCHED" -c scheduler -- \
  airflow dags trigger iceberg_taxi_ingest -r "$RUN_ID" -o plain 2>/dev/null | tail -1

LAST_POD=""
while :; do
  POD=$(kubectl get pods -n data-platform \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null \
    | tr ' ' '\n' | grep iceberg-taxi | head -1)
  if [ -n "$POD" ]; then
    LOGS=$(kubectl logs -n data-platform "$POD" --all-containers --tail=200 2>/dev/null)
    [ -n "$LOGS" ] && printf '%s\n' "$LOGS" > "$OUT"
    kubectl describe pod -n data-platform "$POD" 2>/dev/null \
      > /tmp/worker-$RUN_ID.describe
    kubectl get pod -n data-platform "$POD" -o jsonpath='{.status.containerStatuses}' \
      > /tmp/worker-$RUN_ID.status 2>/dev/null
    LAST_POD=$POD
  elif [ -n "$LAST_POD" ]; then
    echo "pod gone; log -> $OUT"
    exit 0
  fi
  sleep 3
done
