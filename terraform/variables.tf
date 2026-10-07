variable "region" {
  default = "us-east-1"
}

variable "raw_bucket" {
  default = "lake-raw"
}

variable "warehouse_bucket" {
  default = "lake-warehouse"
}

output "buckets" {
  value = [aws_s3_bucket.raw.bucket, aws_s3_bucket.warehouse.bucket]
}
