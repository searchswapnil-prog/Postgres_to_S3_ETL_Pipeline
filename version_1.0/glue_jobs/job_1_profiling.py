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

run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
logger = glueContext.get_logger()

# -------------------------------------------------------
# LOAD CONFIG
# -------------------------------------------------------
s3 = boto3.client("s3")

def load_config(s3_path):
    # s3_path format: s3://bucket/key
    parts  = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key    = parts[1]
    obj    = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

config     = load_config(args["config_s3_path"])
raw_base   = config["s3"]["layers"]["raw"]
logs_base  = config["s3"]["layers"]["logs"]
tables     = config["tables"]
bucket     = config["s3"]["bucket"]

logger.info(f"[Profiling] Job started at {run_ts}")

# -------------------------------------------------------
# HELPER — EMAIL FORMAT CHECK
# -------------------------------------------------------
email_pattern = r'^[\w\.\-]+@[\w\.\-]+\.\w{2,}$'
phone_pattern  = r'^\+?[\d\s\-\(\)]{7,15}$'

# -------------------------------------------------------
# PROFILE ONE TABLE
# -------------------------------------------------------
def profile_table(table_name, table_cfg):
    input_path = f"{raw_base}/{table_name}"
    logger.info(f"[Profiling] Reading table: {table_name} from {input_path}")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        logger.error(f"[Profiling] Could not read {table_name}: {str(e)}")
        return None

    row_count   = df.count()
    col_count   = len(df.columns)
    dup_count   = row_count - df.dropDuplicates().count()

    logger.info(f"[Profiling] {table_name} — rows: {row_count}, duplicates: {dup_count}")

    col_profiles = {}

    for col_name, dtype in df.dtypes:
        col_stat = {
            "dtype":        dtype,
            "null_count":   df.filter(F.col(col_name).isNull()).count(),
            "distinct_count": df.select(col_name).distinct().count()
        }

        # Numeric stats
        if dtype in ("int", "bigint", "double", "float", "decimal(10,2)"):
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.avg(col_name).alias("mean")
            ).collect()[0]
            col_stat["min"]  = str(stats["min"])
            col_stat["max"]  = str(stats["max"])
            col_stat["mean"] = str(round(stats["mean"], 2)) if stats["mean"] else None

        # Email format check
        if col_name in config["validation"]["email_columns"]:
            invalid_email = df.filter(
                F.col(col_name).isNotNull() &
                ~F.col(col_name).rlike(email_pattern)
            ).count()
            col_stat["invalid_email_count"] = invalid_email

        # Phone format check
        if col_name in config["validation"]["phone_columns"]:
            invalid_phone = df.filter(
                F.col(col_name).isNotNull() &
                ~F.col(col_name).rlike(phone_pattern)
            ).count()
            col_stat["invalid_phone_count"] = invalid_phone

        col_profiles[col_name] = col_stat

    return {
        "table":         table_name,
        "run_timestamp": run_ts,
        "row_count":     row_count,
        "column_count":  col_count,
        "duplicate_count": dup_count,
        "columns":       col_profiles
    }

# -------------------------------------------------------
# RUN PROFILING FOR ALL TABLES
# -------------------------------------------------------
full_report = {
    "job":         "job_1_profiling",
    "run_timestamp": run_ts,
    "tables":      {}
}

for table_name, table_cfg in tables.items():
    report = profile_table(table_name, table_cfg)
    if report:
        full_report["tables"][table_name] = report

# -------------------------------------------------------
# WRITE JSON REPORT TO S3 LOGS LAYER
# -------------------------------------------------------
report_key  = f"logs/profiling/profiling_report_{run_ts}.json"
report_body = json.dumps(full_report, indent=4)

s3.put_object(
    Bucket      = bucket,
    Key         = report_key,
    Body        = report_body,
    ContentType = "application/json"
)

logger.info(f"[Profiling] Report saved to s3://{bucket}/{report_key}")
logger.info(f"[Profiling] Job completed at {datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")

job.commit()
