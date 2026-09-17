# Databricks notebook source
source_data = [
    {"order_id": 1, "customer_id": 143, "amount": 283.49, "order_date": "2026-01-18"},
    {"order_id": 2, "customer_id": 132, "amount": 314.23, "order_date": "2026-01-22"},
    {"order_id": 3, "customer_id": 198, "amount": 159.27, "order_date": "2026-01-04"},
]

df = spark.createDataFrame(source_data)
df.show()

# COMMAND ----------

source_data = [
    {"order_id": 1, "customer_id": 143, "amount": 283.49},
    {"order_id": 2, "customer_id": 132, "amount": 314.23},
    {"order_id": 3, "customer_id": 198, "amount": 159.27},
]

target_data = [
    {"order_id": 1, "customer_id": 143, "amount": 283.49},
    {"order_id": 3, "customer_id": 198, "amount": 159.27},
    # order_id 2 is missing on purpose
]

source_df = spark.createDataFrame(source_data)
target_df = spark.createDataFrame(target_data)

source_df.show()
target_df.show()

# COMMAND ----------

missing = source_df.join(target_df, on="order_id", how="left_anti")
missing.show()

# COMMAND ----------

dupe_data = [
    {"order_id": 1, "customer_id": 143, "amount": 283.49},
    {"order_id": 1, "customer_id": 143, "amount": 283.49},  # duplicate
    {"order_id": 2, "customer_id": 132, "amount": 314.23},
]

dupe_df = spark.createDataFrame(dupe_data)
dupe_df.show()

# COMMAND ----------

from pyspark.sql import functions as F

duplicates = dupe_df.groupBy("order_id").count().filter(F.col("count") > 1)
duplicates.show()

# COMMAND ----------

source_data2 = [
    {"order_id": 1, "amount": 100.00},
    {"order_id": 2, "amount": 200.00},
]

target_data2 = [
    {"order_id": 1, "amount": 100.00},
    {"order_id": 2, "amount": 999.00},  # mismatch
]

source_df2 = spark.createDataFrame(source_data2)
target_df2 = spark.createDataFrame(target_data2)

# COMMAND ----------

from pyspark.sql import functions as F

joined = source_df2.alias("s").join(target_df2.alias("t"), on="order_id")

result = joined.withColumn(
    "status",
    F.when(F.col("s.amount") == F.col("t.amount"), "MATCHED")
     .otherwise("MISMATCHED")
)

result.select("order_id", "s.amount", "t.amount", "status").show()

# COMMAND ----------

import boto3
print(boto3.__version__)

# COMMAND ----------

import boto3
import json
import os

s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-2"
)

response = s3.get_object(Bucket="mrunali-dqe-project-20260913", Key="raw/source/source_lines.json")
content = response['Body'].read().decode('utf-8')

# Parse each line as JSON (since it's JSON Lines format)
records = [json.loads(line) for line in content.strip().split('\n')]

df = spark.createDataFrame(records)
df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

response_target = s3.get_object(Bucket="mrunali-dqe-project-20260913", Key="raw/target/target_lines.json")
content_target = response_target['Body'].read().decode('utf-8')
records_target = [json.loads(line) for line in content_target.strip().split('\n')]
target_df = spark.createDataFrame(records_target)

response_consumed = s3.get_object(Bucket="mrunali-dqe-project-20260913", Key="raw/consumed/consumed_orders_lines.json")
content_consumed = response_consumed['Body'].read().decode('utf-8')
records_consumed = [json.loads(line) for line in content_consumed.strip().split('\n')]
consumed_df = spark.createDataFrame(records_consumed)

source_df_real = df  # renaming your existing source data for clarity

print("Source count:", source_df_real.count())
print("Target count:", target_df.count())
print("Consumed count:", consumed_df.count())

# COMMAND ----------

missing_real = source_df_real.join(target_df, on="order_id", how="left_anti")
missing_real.show()

# COMMAND ----------

from pyspark.sql import functions as F

duplicates_real = target_df.groupBy("order_id").count().filter(F.col("count") > 1)
duplicates_real.show()

# COMMAND ----------

joined_real = source_df_real.alias("s").join(target_df.alias("t"), on="order_id")

result_real = joined_real.withColumn(
    "status",
    F.when(F.col("s.amount") == F.col("t.amount"), "MATCHED")
     .otherwise("MISMATCHED")
)

result_real.select("order_id", "s.amount", "t.amount", "status").filter(F.col("status") == "MISMATCHED").show()

# COMMAND ----------

result_real.select(
    "order_id",
    F.col("s.amount").alias("source_amount"),
    F.col("t.amount").alias("target_amount"),
    "status"
).filter(F.col("status") == "MISMATCHED").show()

# COMMAND ----------

