#!/usr/bin/env bash
set -euo pipefail

: "${KLOUDFETCH_S3_BUCKET:=kloudfetch}"
: "${KLOUDFETCH_S3_PREFIX:=results}"
: "${KLOUDFETCH_S3_ENDPOINT_URL:=http://rustfs:9000}"
: "${AWS_ACCESS_KEY_ID:=kloudfetch}"
: "${AWS_SECRET_ACCESS_KEY:=kloudfetch-secret}"
: "${AWS_REGION:=us-east-1}"

exec /opt/spark/bin/spark-submit \
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
  --name "KloudFetch Spark Thrift Server" \
  --master "${SPARK_MASTER:-local[4,4]}" \
  --packages \
    "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},software.amazon.awssdk:bundle:${AWS_SDK_VERSION}" \
  --conf spark.sql.extensions=org.apache.spark.sql.kloudfetch.KloudFetchSparkExtension \
  --conf spark.kloudfetch.bucket="${KLOUDFETCH_S3_BUCKET}" \
  --conf spark.kloudfetch.prefix="${KLOUDFETCH_S3_PREFIX}" \
  --conf spark.kloudfetch.minEstimatedBytes="${KLOUDFETCH_MIN_ESTIMATED_BYTES:-0}" \
  --conf spark.kloudfetch.maxRecordsPerBatch="${KLOUDFETCH_MAX_RECORDS_PER_BATCH:-250000}" \
  --conf spark.kloudfetch.maxEstimatedBatchBytes="${KLOUDFETCH_MAX_ESTIMATED_BATCH_BYTES:-67108864}" \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.sql.execution.arrow.compression.codec="${KLOUDFETCH_ARROW_COMPRESSION:-none}" \
  --conf spark.hadoop.fs.s3a.endpoint="${KLOUDFETCH_S3_ENDPOINT_URL}" \
  --conf spark.hadoop.fs.s3a.endpoint.region="${AWS_REGION}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key="${AWS_ACCESS_KEY_ID}" \
  --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.create.performance=true \
  --conf spark.hadoop.fs.s3a.fast.upload=true \
  --conf spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer \
  --conf spark.hadoop.fs.s3a.multipart.size="${KLOUDFETCH_MULTIPART_SIZE:-16777216}" \
  --conf spark.hadoop.fs.s3a.block.size="${KLOUDFETCH_MULTIPART_SIZE:-16777216}" \
  --conf spark.hadoop.fs.s3a.multipart.threshold="${KLOUDFETCH_MULTIPART_THRESHOLD:-16777216}" \
  --conf spark.hadoop.fs.s3a.server-side-encryption-algorithm="${KLOUDFETCH_SSE_ALGORITHM:-AES256}" \
  --conf spark.task.maxFailures="${KLOUDFETCH_TASK_MAX_FAILURES:-4}" \
  --conf spark.speculation="${KLOUDFETCH_SPECULATION:-true}" \
  --conf spark.speculation.interval="${KLOUDFETCH_SPECULATION_INTERVAL:-100ms}" \
  --conf spark.speculation.multiplier="${KLOUDFETCH_SPECULATION_MULTIPLIER:-1.5}" \
  --conf spark.speculation.quantile="${KLOUDFETCH_SPECULATION_QUANTILE:-0.5}" \
  --conf spark.executor.cores="${SPARK_EXECUTOR_CORES:-2}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf spark.driver.host="${SPARK_DRIVER_HOST:-spark}" \
  --conf spark.kloudfetch.test.failFirstAttemptPartition="${KLOUDFETCH_TEST_FAIL_FIRST_ATTEMPT_PARTITION:--1}" \
  --conf spark.kloudfetch.test.slowFirstAttemptPartition="${KLOUDFETCH_TEST_SLOW_FIRST_ATTEMPT_PARTITION:--1}" \
  --conf spark.kloudfetch.test.slowFirstAttemptMillis="${KLOUDFETCH_TEST_SLOW_FIRST_ATTEMPT_MILLIS:-0}" \
  --conf spark.kloudfetch.test.failDuringUploadPartition="${KLOUDFETCH_TEST_FAIL_DURING_UPLOAD_PARTITION:--1}" \
  --conf spark.sql.shuffle.partitions="${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --conf spark.ui.enabled=false \
  --hiveconf hive.server2.transport.mode=http \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.thrift.http.port=10001 \
  --hiveconf hive.server2.http.endpoint=cliservice \
  --hiveconf "javax.jdo.option.ConnectionURL=jdbc:derby:/tmp/kloudfetch-metastore/metastore_db;create=true" \
  --hiveconf hive.server2.authentication=NONE
