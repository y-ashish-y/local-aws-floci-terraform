"""Synthetic NYC-taxi ingest into Iceberg (Nessie REST + S3 via floci)."""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, round as sround, col, expr, days

ICEBERG_PKGS = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.107.5"
)


def session(nessie_uri, s3_endpoint):
    return (
        SparkSession.builder.appName("taxi-ingest")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "lake")
        .config("spark.sql.catalog.nessie.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.nessie.s3.access-key-id", "test")
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "test")
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main(nessie_uri, s3_endpoint, rows, branch="main"):
    spark = session(nessie_uri, s3_endpoint)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    df = (
        spark.range(rows)
        .withColumn("pickup_ts", expr("timestamp_millis(1704067200000 + id * 1000)"))
        .withColumn("trip_miles", sround(rand() * 20 + 0.5, 2))
        .withColumn("fare_amount", sround(rand() * 60 + 3, 2))
        .withColumn("passenger_count", (rand() * 4 + 1).cast("int"))
        .withColumn("payment_type", (rand() * 2 + 1).cast("int"))
        .select(col("id").alias("trip_id"), "pickup_ts", "trip_miles", "fare_amount", "passenger_count", "payment_type")
    )
    df.writeTo("nessie.bronze.taxi_trips").using("iceberg").partitionedBy(days(col("pickup_ts"))).createOrReplace()
    print(f"WROTE rows={df.count()} to nessie.bronze.taxi_trips")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--nessie-uri", default="http://nessie.nessie:19120/iceberg/")
    p.add_argument("--s3-endpoint", default="http://host.docker.internal:4566")
    p.add_argument("--rows", type=int, default=10000)
    p.add_argument("--branch", default="main")
    main(**vars(p.parse_args()))
