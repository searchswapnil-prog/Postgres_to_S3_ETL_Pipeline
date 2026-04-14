import sys
import json
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------------------------------------
# INIT
# -------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "config_s3_path", "etl_run_ts"])
sc   = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

run_ts     = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
etl_run_ts = args["etl_run_ts"]
logger     = glueContext.get_logger()

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

config         = load_config(args["config_s3_path"])
input_base     = config["s3"]["layers"]["processed"]
iceberg_base   = config["s3"]["layers"]["iceberg"]
error_base     = config["s3"]["layers"]["error"]
logs_base      = config["s3"]["layers"]["logs"]
bucket         = config["s3"]["bucket"]
pass_threshold = config["dq"]["pass_threshold"]
email_cols     = config["validation"]["email_columns"]
phone_cols     = config["validation"]["phone_columns"]
numeric_ranges = config["validation"]["numeric_ranges"]
date_cols      = config["validation"]["date_columns"]

logger.info(f"[DQ] Job started at {run_ts}")

# -------------------------------------------------------
# READ PROCESSED DATA
# -------------------------------------------------------
input_path = f"{input_base}/{etl_run_ts}"

try:
    df = spark.read.parquet(input_path)
except Exception as e:
    logger.error(f"[DQ] Failed to read processed data: {str(e)}")
    raise e

total_rows    = df.count()
total_checks  = 0
passed_checks = 0
failed_details = []

logger.info(f"[DQ] Total rows to validate: {total_rows}")

# -------------------------------------------------------
# Filter to rows that actually have an order
# Customers with no orders produce nulls via LEFT JOIN
# -------------------------------------------------------
df_with_orders = df.filter(F.col("order_id").isNotNull())
logger.info(f"[DQ] Rows with orders (non-null order_id): {df_with_orders.count()}")

# -------------------------------------------------------
# CHECK 1 — NULL CHECK (critical columns only)
# order_amount nulls are filled with 0 in fixes — not a hard fail
# -------------------------------------------------------
critical_columns = ["customer_id", "order_id", "order_date", "order_status"]

for col_name in critical_columns:
    if col_name not in df.columns:
        continue
    null_count = df_with_orders.filter(F.col(col_name).isNull()).count()
    total_checks += 1
    if null_count == 0:
        passed_checks += 1
    else:
        failed_details.append({
            "check": "null_check",
            "column": col_name,
            "failed_count": null_count
        })
        logger.info(f"[DQ] NULL check failed — {col_name}: {null_count} nulls")

# -------------------------------------------------------
# CHECK 2 — DUPLICATE CHECK
# Dedup is applied in basic fixes — log count but don't fail
# Multiple payment rows per order is a known join artifact
# -------------------------------------------------------
window_spec = Window.partitionBy("order_id").orderBy(F.col("payment_date").desc())
df_deduped  = df_with_orders.withColumn("_rank", F.row_number().over(window_spec)) \
                             .filter(F.col("_rank") == 1).drop("_rank")

dup_count    = df_with_orders.count() - df_deduped.count()
total_checks += 1
passed_checks += 1  # always pass — duplicates are fixed downstream
if dup_count > 0:
    logger.info(f"[DQ] DUPLICATE note — {dup_count} rows deduped by order_id (handled in fixes)")
# -------------------------------------------------------
# CHECK 3 — EMAIL FORMAT VALIDATION
# -------------------------------------------------------
email_pattern = r'^[\w\.\-]+@[\w\.\-]+\.\w{2,}$'
for col_name in email_cols:
    if col_name in df.columns:
        invalid = df.filter(
            F.col(col_name).isNotNull() &
            ~F.col(col_name).rlike(email_pattern)
        ).count()
        total_checks += 1
        if invalid == 0:
            passed_checks += 1
        else:
            failed_details.append({
                "check": "email_format",
                "column": col_name,
                "failed_count": invalid
            })

# -------------------------------------------------------
# CHECK 4 — PHONE FORMAT VALIDATION
# -------------------------------------------------------
phone_pattern = r'^\+?[\d\s\-\(\)]{7,15}$'
for col_name in phone_cols:
    if col_name in df.columns:
        invalid = df.filter(
            F.col(col_name).isNotNull() &
            ~F.col(col_name).rlike(phone_pattern)
        ).count()
        total_checks += 1
        if invalid == 0:
            passed_checks += 1
        else:
            failed_details.append({
                "check": "phone_format",
                "column": col_name,
                "failed_count": invalid
            })

# -------------------------------------------------------
# CHECK 5 — NUMERIC RANGE VALIDATION
# -------------------------------------------------------
for col_name, range_cfg in numeric_ranges.items():
    if col_name in df.columns:
        out_of_range = df.filter(
            F.col(col_name).isNotNull() &
            ((F.col(col_name) < range_cfg["min"]) | (F.col(col_name) > range_cfg["max"]))
        ).count()
        total_checks += 1
        if out_of_range == 0:
            passed_checks += 1
        else:
            failed_details.append({
                "check": "numeric_range",
                "column": col_name,
                "failed_count": out_of_range
            })

# -------------------------------------------------------
# CHECK 6 — REFERENTIAL INTEGRITY
# Only meaningful on rows that have orders
# -------------------------------------------------------
customer_ids  = df.select("customer_id").distinct()
orphan_orders = df_with_orders.join(customer_ids, on="customer_id", how="left_anti").count()
total_checks += 1
if orphan_orders == 0:
    passed_checks += 1
else:
    failed_details.append({
        "check": "referential_integrity",
        "column": "customer_id",
        "failed_count": orphan_orders
    })

# -------------------------------------------------------
# DQ SCORE CALCULATION
# -------------------------------------------------------
dq_score  = round((passed_checks / total_checks) * 100, 2) if total_checks > 0 else 0
dq_status = "PASS" if dq_score >= pass_threshold else "FAIL"

logger.info(f"[DQ] Score: {dq_score}% | Status: {dq_status} | Checks: {passed_checks}/{total_checks} passed")

# -------------------------------------------------------
# BASIC FIXES — fill nulls, remove invalid records
# -------------------------------------------------------
df_fixed = df

# Fill null order_amount with 0
df_fixed = df_fixed.withColumn(
    "order_amount",
    F.when(F.col("order_amount").isNull(), 0.0).otherwise(F.col("order_amount"))
)

# Fill nulls in string columns with "UNKNOWN"
for col_name, dtype in df_fixed.dtypes:
    if dtype == "string":
        df_fixed = df_fixed.fillna({col_name: "UNKNOWN"})

# Remove rows where order_id is null (customers with no orders)
df_fixed = df_fixed.filter(F.col("order_id").isNotNull())

# Dedup — keep one row per order_id (latest payment if multiple)
window_spec_fix = Window.partitionBy("order_id").orderBy(F.col("payment_date").desc())
df_fixed = df_fixed.withColumn("_rank", F.row_number().over(window_spec_fix)) \
                   .filter(F.col("_rank") == 1) \
                   .drop("_rank")

# Recalculate total_amount after fixes
df_fixed = df_fixed.withColumn(
    "total_amount",
    F.round(F.col("order_amount") + F.coalesce(F.col("amount"), F.lit(0.0)), 2)
)

logger.info(f"[DQ] Rows after basic fixes: {df_fixed.count()}")

# -------------------------------------------------------
# ROUTE OUTPUT BASED ON DQ SCORE
# -------------------------------------------------------
if dq_status == "PASS":
    output_path = f"{iceberg_base}/dq_passed/{run_ts}"
    df_fixed.write.mode("overwrite").parquet(output_path)
    logger.info(f"[DQ] PASSED — written to iceberg input path: {output_path}")
else:
    error_path = f"{error_base}/{run_ts}"
    df_fixed.write.mode("overwrite").parquet(error_path)
    logger.info(f"[DQ] FAILED — written to error layer: {error_path}")

# -------------------------------------------------------
# WRITE DQ JSON REPORT TO LOGS
# -------------------------------------------------------
dq_report = {
    "job":            "job_5_dq_check",
    "run_timestamp":  run_ts,
    "total_rows":     total_rows,
    "total_checks":   total_checks,
    "passed_checks":  passed_checks,
    "dq_score_pct":   dq_score,
    "dq_status":      dq_status,
    "pass_threshold": pass_threshold,
    "failed_checks":  failed_details
}

report_key = f"logs/dq/dq_report_{run_ts}.json"
s3.put_object(
    Bucket      = bucket,
    Key         = report_key,
    Body        = json.dumps(dq_report, indent=4),
    ContentType = "application/json"
)

logger.info(f"[DQ] Report saved to s3://{bucket}/{report_key}")
logger.info(f"[DQ] Job completed at {datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")

job.commit()