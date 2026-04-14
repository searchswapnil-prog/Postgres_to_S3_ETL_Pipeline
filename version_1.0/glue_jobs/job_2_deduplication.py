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
    parts  = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key    = parts[1]
    obj    = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

config      = load_config(args["config_s3_path"])
raw_base    = config["s3"]["layers"]["raw"]
output_base = config["s3"]["layers"]["staging_dedup"]
tables      = config["tables"]

logger.info(f"[Dedup] Job started at {run_ts}")

# -------------------------------------------------------
# DEDUP ONE TABLE USING WINDOW FUNCTION
# -------------------------------------------------------
def dedup_table(table_name, table_cfg):
    input_path  = f"{raw_base}/{table_name}"
    output_path = f"{output_base}/{table_name}/{run_ts}"
    pk          = table_cfg["primary_key"]
    sort_col    = table_cfg["sort_column"]

    logger.info(f"[Dedup] Processing {table_name} | PK: {pk} | Sort: {sort_col}")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        logger.error(f"[Dedup] Could not read {table_name}: {str(e)}")
        return

    count_before = df.count()

    # Window function — rank rows within each primary key group
    # Highest sort_column value = latest record = rank 1
    window_spec = Window.partitionBy(pk).orderBy(F.col(sort_col).desc())

    df_ranked = df.withColumn("_row_rank", F.row_number().over(window_spec))

    # Keep only rank 1 (latest record per primary key)
    df_deduped = df_ranked.filter(F.col("_row_rank") == 1).drop("_row_rank")

    count_after   = df_deduped.count()
    removed_count = count_before - count_after

    logger.info(f"[Dedup] {table_name} — before: {count_before}, after: {count_after}, removed: {removed_count}")

    # Write deduplicated data to staging
    df_deduped.write.mode("overwrite").parquet(output_path)

    logger.info(f"[Dedup] {table_name} written to {output_path}")

# -------------------------------------------------------
# RUN DEDUP FOR ALL TABLES
# -------------------------------------------------------
for table_name, table_cfg in tables.items():
    dedup_table(table_name, table_cfg)

logger.info(f"[Dedup] Job completed at {datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
job.commit()
