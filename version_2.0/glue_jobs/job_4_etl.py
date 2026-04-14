import sys
import json
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# -------------------------------------------------------
# INIT
# -------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "config_s3_path"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

run_ts        = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
logger        = glueContext.get_logger()

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--WORKFLOW_NAME', default=None)
parser.add_argument('--WORKFLOW_RUN_ID', default=None)
known_args, _ = parser.parse_known_args()

if known_args.WORKFLOW_NAME and known_args.WORKFLOW_RUN_ID:
    glue_client = boto3.client('glue')
    response = glue_client.get_workflow_run_properties(Name=known_args.WORKFLOW_NAME, RunId=known_args.WORKFLOW_RUN_ID)
    props = response.get('RunProperties', {})
    masked_run_ts = props.get('masked_run_ts')
else:
    args_fallback = getResolvedOptions(sys.argv, ["masked_run_ts"])
    masked_run_ts = args_fallback["masked_run_ts"]


# -------------------------------------------------------
# LOAD CONFIG
# -------------------------------------------------------
s3 = boto3.client("s3")

def load_config(s3_path):
    parts  = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key    = parts[1]
    obj    = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

config       = load_config(args["config_s3_path"])
input_base   = config["s3"]["layers"]["staging_masked"]
output_base  = config["s3"]["layers"]["processed"]
date_format  = config["etl"]["date_format"]
join_key     = config["etl"]["join_key"]
drop_columns = config["etl"]["drop_columns"]

logger.info(f"[ETL] Job started at {run_ts}")

# -------------------------------------------------------
# READ ALL 4 MASKED TABLES
# -------------------------------------------------------
try:
    df_customer = spark.read.parquet(f"{input_base}/customer/{masked_run_ts}")
    df_address  = spark.read.parquet(f"{input_base}/address/{masked_run_ts}")
    df_orders   = spark.read.parquet(f"{input_base}/orders/{masked_run_ts}")
    df_payments = spark.read.parquet(f"{input_base}/payments/{masked_run_ts}")
except Exception as e:
    logger.error(f"[ETL] Failed to read masked tables: {str(e)}")
    raise e

# -------------------------------------------------------
# STEP 1 — DATA TYPE CASTING & DATE STANDARDIZATION
# -------------------------------------------------------
df_orders = (
    df_orders
    .withColumn("order_date",   F.to_date(F.col("order_date"),   date_format))
    .withColumn("order_amount", F.col("order_amount").cast("double"))
    .withColumn("order_status", F.trim(F.lower(F.col("order_status"))))
)

df_payments = (
    df_payments
    .withColumn("payment_date",   F.to_date(F.col("payment_date"), date_format))
    .withColumn("amount",         F.col("amount").cast("double"))
    .withColumn("payment_method", F.trim(F.lower(F.col("payment_method"))))
)

logger.info("[ETL] Data types cast and dates standardized")

# -------------------------------------------------------
# STEP 2 — COLUMN RENAMING (consistent naming convention)
# -------------------------------------------------------
df_customer = (
    df_customer
    .withColumnRenamed("first_name", "customer_first_name")
    .withColumnRenamed("last_name",  "customer_last_name")
    .withColumnRenamed("email",      "customer_email")
    .withColumnRenamed("phone",      "customer_phone")
)

df_address = (
    df_address
    .withColumnRenamed("street_address", "customer_street")
    .withColumnRenamed("city",           "customer_city")
    .withColumnRenamed("state",          "customer_state")
    .withColumnRenamed("zip_code",       "customer_zip")
)

logger.info("[ETL] Columns renamed for consistent naming")

# -------------------------------------------------------
# STEP 3 — JOIN ALL 4 TABLES
# customer → address → orders → payments
# -------------------------------------------------------
df_cust_addr = df_customer.join(
    df_address.drop("address_id"),
    on=join_key,
    how="left"
)

df_with_orders = df_cust_addr.join(
    df_orders,
    on=join_key,
    how="left"
)

df_full = df_with_orders.join(
    df_payments,
    on="order_id",
    how="left"
)

logger.info(f"[ETL] All 4 tables joined | Row count: {df_full.count()}")

# -------------------------------------------------------
# STEP 4 — DERIVED COLUMNS
# -------------------------------------------------------
# total_amount = order_amount + payment amount
df_full = df_full.withColumn(
    "total_amount",
    F.round(F.col("order_amount") + F.col("amount"), 2)
)

logger.info("[ETL] Derived column total_amount added")

# -------------------------------------------------------
# STEP 5 — REMOVE UNWANTED COLUMNS
# -------------------------------------------------------
if drop_columns:
    df_full = df_full.drop(*drop_columns)
    logger.info(f"[ETL] Dropped columns: {drop_columns}")

# -------------------------------------------------------
# STEP 6 — WRITE TO PROCESSED LAYER
# -------------------------------------------------------
output_path = f"{output_base}/{run_ts}"
df_full.write.mode("overwrite").parquet(output_path)

logger.info(f"[ETL] Processed data written to {output_path}")
logger.info(f"[ETL] Final row count: {df_full.count()}")
logger.info(f"[ETL] Job completed at {datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")

if known_args.WORKFLOW_NAME and known_args.WORKFLOW_RUN_ID:
    response = glue_client.get_workflow_run_properties(Name=known_args.WORKFLOW_NAME, RunId=known_args.WORKFLOW_RUN_ID)
    props = response.get('RunProperties', {})
    props['etl_run_ts'] = run_ts
    glue_client.put_workflow_run_properties(Name=known_args.WORKFLOW_NAME, RunId=known_args.WORKFLOW_RUN_ID, RunProperties=props)
    logger.info(f"Workflow properties updated with etl_run_ts={run_ts}")

job.commit()
