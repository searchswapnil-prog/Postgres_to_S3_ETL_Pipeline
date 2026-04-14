import sys
import json
import boto3
import hashlib
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

run_ts      = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
logger      = glueContext.get_logger()

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--WORKFLOW_NAME', default=None)
parser.add_argument('--WORKFLOW_RUN_ID', default=None)
known_args, _ = parser.parse_known_args()

if known_args.WORKFLOW_NAME and known_args.WORKFLOW_RUN_ID:
    glue_client = boto3.client('glue')
    response = glue_client.get_workflow_run_properties(Name=known_args.WORKFLOW_NAME, RunId=known_args.WORKFLOW_RUN_ID)
    props = response.get('RunProperties', {})
    dedup_run_ts = props.get('dedup_run_ts')
else:
    args_fallback = getResolvedOptions(sys.argv, ["dedup_run_ts"])
    dedup_run_ts = args_fallback["dedup_run_ts"]


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

config        = load_config(args["config_s3_path"])
input_base    = config["s3"]["layers"]["staging_dedup"]
output_base   = config["s3"]["layers"]["staging_masked"]
tables        = config["tables"]
masking_rules = config["pii"]["masking_rules"]
hash_columns  = config["pii"]["hash_columns"]

logger.info(f"[PII Masking] Job started at {run_ts}")

# -------------------------------------------------------
# MASKING FUNCTIONS
# -------------------------------------------------------

# Partial mask — keeps first 2 and last 2 chars, masks the middle
def partial_mask_udf(value):
    if value is None:
        return None
    s = str(value)
    if len(s) <= 4:
        return "*" * len(s)
    return s[:2] + "*" * (len(s) - 4) + s[-2:]

partial_mask = F.udf(partial_mask_udf)

# Email partial mask — masks local part before @
def mask_email_udf(value):
    if value is None:
        return None
    if "@" not in str(value):
        return "****@****.***"
    local, domain = str(value).split("@", 1)
    masked_local  = local[:2] + "*" * (len(local) - 2) if len(local) > 2 else "**"
    return f"{masked_local}@{domain}"

mask_email = F.udf(mask_email_udf)

# SHA-256 hash
def sha256_udf(value):
    if value is None:
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()

sha256_hash = F.udf(sha256_udf)

# -------------------------------------------------------
# MASK ONE TABLE
# -------------------------------------------------------
def mask_table(table_name, table_cfg):
    input_path  = f"{input_base}/{table_name}/{dedup_run_ts}"
    output_path = f"{output_base}/{table_name}/{run_ts}"
    pii_columns = table_cfg["pii_columns"]

    if not pii_columns:
        logger.info(f"[PII Masking] {table_name} — no PII columns, copying as-is")
        try:
            df = spark.read.parquet(input_path)
            df.write.mode("overwrite").parquet(output_path)
        except Exception as e:
            logger.error(f"[PII Masking] Could not process {table_name}: {str(e)}")
        return

    logger.info(f"[PII Masking] Processing {table_name} | PII cols: {pii_columns}")

    try:
        df = spark.read.parquet(input_path)
    except Exception as e:
        logger.error(f"[PII Masking] Could not read {table_name}: {str(e)}")
        return

    masked_cols = []

    for col_name in pii_columns:
        rule = masking_rules.get(col_name, "partial")

        if col_name == "email":
            df = df.withColumn(col_name, mask_email(F.col(col_name)))
        elif rule == "partial":
            df = df.withColumn(col_name, partial_mask(F.col(col_name)))

        # Additionally hash email columns
        if col_name in hash_columns:
            df = df.withColumn(f"{col_name}_hashed", sha256_hash(F.col(col_name)))

        masked_cols.append(col_name)
        logger.info(f"[PII Masking] {table_name}.{col_name} masked with rule: {rule}")

    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"[PII Masking] {table_name} written to {output_path}")

# -------------------------------------------------------
# RUN MASKING FOR ALL TABLES
# -------------------------------------------------------
for table_name, table_cfg in tables.items():
    mask_table(table_name, table_cfg)

logger.info(f"[PII Masking] Job completed at {datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")

if known_args.WORKFLOW_NAME and known_args.WORKFLOW_RUN_ID:
    response = glue_client.get_workflow_run_properties(Name=known_args.WORKFLOW_NAME, RunId=known_args.WORKFLOW_RUN_ID)
    props = response.get('RunProperties', {})
    props['masked_run_ts'] = run_ts
    glue_client.put_workflow_run_properties(Name=known_args.WORKFLOW_NAME, RunId=known_args.WORKFLOW_RUN_ID, RunProperties=props)
    logger.info(f"Workflow properties updated with masked_run_ts={run_ts}")

job.commit()
