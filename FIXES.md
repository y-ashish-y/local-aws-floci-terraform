# Fixes applied while bringing the stack up

Each entry: symptom → root cause → fix (already baked into the repo).

## 1. `terraform` missing on Linuxbrew
- Symptom: `brew install terraform` → "No available formula".
- Cause: HashiCorp license change removed it from Homebrew core.
- Fix: `scripts/install-tools.sh` downloads the Terraform binary from
  `releases.hashicorp.com` into `~/.local/bin`.

## 2. Docker permission denied
- Symptom: `docker ps` → permission denied on `/var/run/docker.sock`.
- Cause: user not in `docker` group / wrong context (`desktop-linux`).
- Fix: `docker context use default`; `floci start` pulled the image and the
  daemon became reachable. (If it recurs: `sudo usermod -aG docker $USER`.)

## 3. spark-operator ignored SparkApplications in `data-platform`
- Symptom: app created, no driver pod, no events, empty status.
- Cause: chart default `spark.jobNamespaces: [default]` — controller watches
  only `default` (`--namespaces=default` in controller args).
- Fix: install with `--set 'spark.jobNamespaces={data-platform}'`
  (in `scripts/setup.sh`).

## 4. `deps.packages` failed: Ivy home `/nonexistent` not writable
- Symptom: `FileNotFoundException: /nonexistent/.ivy2.5.2/...` during submit.
- Cause: `apache/spark` image runs as UID 185 with `HOME=/nonexistent`;
  `--packages` resolution via Ivy needs a writable home.
- Fix: `k8s/spark-application-template.yaml` uses `deps.jars` with direct
  Maven Central URLs instead of `deps.packages` (no Ivy involved).

## 5. `nessie-spark-extensions` jar 404 on Maven Central
- Symptom: driver `FileNotFoundException` for
  `nessie-spark-extensions-3.5_2.12-0.107.5.jar`.
- Cause: that artifact/version does not exist at that path, and the job
  never uses Nessie SQL extensions anyway.
- Fix: dropped the jar and the `NessieSparkSessionExtensions` extension;
  `iceberg-spark-runtime` alone is enough for REST-catalog ops.

## 6. Iceberg 1.11 needs Java 17, image runs Java 11
- Symptom: `UnsupportedClassVersionError ... class file version 61.0 ...
  recognizes up to 55.0`.
- Cause: Iceberg 1.11 dropped Java 11; `apache/spark:3.5.6` runs Java 11.
- Fix: `iceberg-spark-runtime-3.5_2.12:1.10.0` (last Java-11-compatible line).

## 7. `RESTSessionCatalog does not implement Catalog`
- Symptom: `IllegalArgumentException: Cannot initialize Catalog`.
- Cause: `SparkCatalog` requires a `TableCatalog`; `RESTSessionCatalog`
  is a session catalog.
- Fix: `spark.sql.catalog.nessie.catalog-impl =
  org.apache.iceberg.rest.RESTCatalog` (`spark/jobs/ingest_taxi.py`).

## 8. Nessie: `Warehouse '...' is not known` (3 rounds)
- (a) Client sent `s3a://lake-warehouse/wh`, server declares `s3://...`.
- (b) Server matches by warehouse **name**, not URL.
- (c) Real cause: `catalog.enabled: false` by default in Nessie chart
  0.108.8 — our `warehouses:` values were silently ignored
  (verified with `helm template` rendering zero warehouse lines).
- Fix: `helm/values-nessie.yaml` sets `catalog.enabled: true`,
  `defaultWarehouse: lake`, warehouse `lake → s3://lake-warehouse/wh`,
  S3 `endpoint: http://host.docker.internal:4566` + path-style + static
  `test/test` creds via `nessie-s3-creds` secret; client sends
  `spark.sql.catalog.nessie.warehouse = lake`.

## 9. `S3FileIO` missing AWS SDK classes
- Symptom: `NoClassDefFoundError: software/amazon/awssdk/...SdkException`.
- Cause: `iceberg-spark-runtime` does not bundle the AWS SDK.
- Fix: added `iceberg-aws-bundle-1.10.0.jar` to `deps.jars`, plus catalog
  props `s3.endpoint / s3.access-key-id / s3.secret-access-key /
  s3.path-style-access / client.region` in the job.

## 10. `partitionedBy("days(pickup_ts)")` SQL syntax error
- Symptom: `[PARSE_SYNTAX_ERROR] ... near '('`.
- Cause: `DataFrameWriterV2.partitionedBy` needs `Column` transforms,
  not SQL strings.
- Fix: `.partitionedBy(days(col("pickup_ts")))` with
  `from pyspark.sql.functions import days`.

## 11. Table identifier must not contain the branch
- Symptom: `AlreadyExistsException: namespace 'main.bronze' must exist,
  namespace 'main' must exist`.
- Cause: job addressed `nessie.main.bronze.taxi_trips`; the ref comes from
  `spark.sql.catalog.nessie.ref=main`, so `main` was parsed as a namespace.
- Fix: address `nessie.bronze.taxi_trips`.

## 12. Scheduler lacks the Spark provider -> use stock image + cncf provider
- Symptom: `ModuleNotFoundError: No module named 'airflow.providers.apache'`.
- Cause: chart `extraPipPackages` installs into worker pods only.
- First attempt (reverted): custom `docker/airflow` image with the spark
  provider baked in. Abandoned because provider v6/5.x/4.x no longer ships
  `operators.spark_kubernetes` at all (verified absent in 6.3.2, 5.6.0,
  4.9.0) — it moved to `airflow.providers.cncf.kubernetes`, which is
  already in the stock `apache/airflow:3.2.2` image.
- Fix: DAG imports from
  `airflow.providers.cncf.kubernetes.{operators,sensors}.spark_kubernetes`;
  no custom image, stock chart image.

## 13. DAG files copied to the wrong pod
- Symptom: dag-processor stats show 0 files, DAG never appears.
- Cause: `kubectl cp` went to the scheduler pod, which does **not** mount
  the `airflow-dags` PVC; only dag-processor does.
- Fix: copy `iceberg_ingest_dag.py` + `spark-application-template.yaml`
  into the dag-processor pod (`scripts/setup.sh`); DAG loads the template
  from its own directory.

## Verified end to end
- `SparkApplication taxi-ingest-manual13` → `COMPLETED`,
  driver log `WROTE rows=10000 to nessie.bronze.taxi_trips`,
  `s3://lake-warehouse/wh/bronze/taxi_trips_*/` holds parquet + avro +
  `metadata.json` in floci.
- Airflow UI at http://localhost:8080 (admin/admin).

## 14. `days_ago` removed in Airflow 3
- Symptom: `ImportError: cannot import name 'days_ago'`.
- Fix: `start_date=pendulum.datetime(2026, 10, 7, tz="UTC")`.

## 15. `application_file` takes a path, not a dict
- Symptom: `AttributeError: 'dict' object has no attribute 'rstrip'` in
  `manage_template_specs`.
- Cause: this provider version only accepts a path string (or raw YAML
  string) for `application_file`; file content is not Jinja-rendered.
- Fix: pass the loaded dict via `template_spec` instead — it IS a
  rendered template field, so `{{ ds_nodash }}` in the app name resolves
  per run.

## 16. Missing `kubernetes_default` connection
- Symptom: submit task fails in seconds.
- Fix: `helm/values-airflow.yaml` sets
  `AIRFLOW_CONN_KUBERNETES_DEFAULT='{"conn_type":"kubernetes","extra":{"in_cluster":true}}'`.

## 17. Airflow SAs forbidden from creating SparkApplications
- Symptom: `403 ... airflow-scheduler cannot create resource
  sparkapplications`.
- Fix: `k8s/airflow-spark-rbac.yaml` (ClusterRole + binding for
  `airflow-scheduler`/`airflow-worker`); applied in `scripts/setup.sh`.

## 18. Airflow UI not reachable on localhost:8080
- Symptom: `curl localhost:8080` → 000; `svc/airflow-api-server` is
  `ClusterIP` (chart `web.service` values don't apply to Airflow 3's
  api-server, and its `apiServer.service` block has no nodePort field).
- Fix: `kubectl patch svc airflow-api-server` to `NodePort 30080`
  (kind forwards node 30080 → host 8080); step added to `scripts/setup.sh`.

## 19. Nessie OOMKilled on a 3.5GB Docker Desktop VM
- Symptom: `Reason: OOMKilled, Exit 137`, `docker info` shows
  `Total Memory: 3.52GiB` while the stack needs ~5GB at Spark-run peaks.
- Fix (fit the box): `JAVA_OPTS_APPEND=-Xmx768m` for Nessie
  (`helm/values-nessie.yaml`), Spark driver/executor `1g` → `768m`
  (`k8s/spark-application-template.yaml`). Long term: raise Docker
  Desktop memory.
