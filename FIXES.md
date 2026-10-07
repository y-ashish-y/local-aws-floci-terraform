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

## 12. Scheduler lacks the Spark provider
- Symptom: `ModuleNotFoundError: No module named 'airflow.providers.apache'`
  in scheduler; DAG never parses.
- Cause: chart `extraPipPackages` installs into worker pods only, not the
  scheduler/dag-processor.
- Fix: `docker/airflow/Dockerfile` (`FROM apache/airflow:3.2.2` + pip
  install spark provider), referenced in `helm/values-airflow.yaml`
  (`images.airflow: lake-airflow:3.2.2-spark`).

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
