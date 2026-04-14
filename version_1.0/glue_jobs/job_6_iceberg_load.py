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
args = getResolvedOptions(sys.argv, ["JOB_NAME", "config_s3_path", "dq_run_ts", "dq_status"])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

run_ts    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
dq_run_ts = args["dq_run_ts"]
dq_status = args["dq_status"]
logger    = glueContext.get_logger()

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
iceberg_base = config["s3"]["layers"]["iceberg"]
iceberg_cfg  = config["iceberg"]
database     = iceberg_cfg["database"]
warehouse    = iceberg_cfg["warehouse"]
region       = config["aws"]["region"]

logger.info(f"[Iceberg Load] Job started at {run_ts}")
logger.info(f"[Iceberg Load] DQ status received: {dq_status}")

# -------------------------------------------------------
# GATE — Only proceed if DQ passed
# -------------------------------------------------------
if dq_status != "PASS":
    logger.info(f"[Iceberg Load] DQ status is {dq_status}. Skipping Iceberg load.")
    job.commit()
    sys.exit(0)

# -------------------------------------------------------
# READ DQ-PASSED DATA (the full joined DataFrame from Job 5)
# -------------------------------------------------------
input_path = f"{iceberg_base}/dq_passed/{dq_run_ts}"
logger.info(f"[Iceberg Load] Reading from: {input_path}")

try:
    df = spark.read.parquet(input_path)
except Exception as e:
    logger.error(f"[Iceberg Load] Failed to read DQ passed data: {str(e)}")
    raise e

row_count = df.count()
logger.info(f"[Iceberg Load] Total rows in DQ-passed data: {row_count}")

# -------------------------------------------------------
# SCHEMA VALIDATION — ensure critical columns exist
# -------------------------------------------------------
required_columns = [
    "customer_id", "order_id", "order_date",
    "order_amount", "total_amount", "payment_id"
]
missing_cols = [c for c in required_columns if c not in df.columns]
if missing_cols:
    raise ValueError(f"[Iceberg Load] Missing required columns: {missing_cols}")

logger.info("[Iceberg Load] Schema validation passed")

# -------------------------------------------------------
# CAST ALL COLUMNS TO ICEBERG-COMPATIBLE TYPES
# Based on confirmed parquet schema from dq_passed file
# -------------------------------------------------------
df = (df
    .withColumn("order_id",            F.col("order_id").cast("int"))
    .withColumn("customer_id",         F.col("customer_id").cast("int"))
    .withColumn("customer_first_name", F.col("customer_first_name").cast("string"))
    .withColumn("customer_last_name",  F.col("customer_last_name").cast("string"))
    .withColumn("customer_email",      F.col("customer_email").cast("string"))
    .withColumn("customer_phone",      F.col("customer_phone").cast("string"))
    .withColumn("email_hashed",        F.col("email_hashed").cast("string"))
    .withColumn("customer_street",     F.col("customer_street").cast("string"))
    .withColumn("customer_city",       F.col("customer_city").cast("string"))
    .withColumn("customer_state",      F.col("customer_state").cast("string"))
    .withColumn("customer_zip",        F.col("customer_zip").cast("string"))
    .withColumn("order_date",          F.to_date(F.col("order_date")))
    .withColumn("order_amount",        F.col("order_amount").cast("double"))
    .withColumn("order_status",        F.col("order_status").cast("string"))
    .withColumn("payment_id",          F.col("payment_id").cast("int"))
    .withColumn("payment_date",        F.to_date(F.col("payment_date")))
    .withColumn("amount",              F.col("amount").cast("double"))
    .withColumn("payment_method",      F.col("payment_method").cast("string"))
    .withColumn("total_amount",        F.col("total_amount").cast("double"))
)

logger.info("[Iceberg Load] All columns cast to Iceberg-compatible types")

# -------------------------------------------------------
# CREATE GLUE DATABASE IF IT DOESN'T EXIST
# -------------------------------------------------------
glue_client = boto3.client("glue", region_name=region)

try:
    glue_client.get_database(Name=database)
    logger.info(f"[Iceberg Load] Database already exists: {database}")
except glue_client.exceptions.EntityNotFoundException:
    glue_client.create_database(
        DatabaseInput={
            "Name":        database,
            "Description": "Iceberg database for curated customer pipeline data"
        }
    )
    logger.info(f"[Iceberg Load] Created database: {database}")

# -------------------------------------------------------
# REGISTER MASTER TEMP VIEW
# All 4 table splits are done via SQL SELECT from this view
# -------------------------------------------------------
df.createOrReplaceTempView("dq_passed_data")

# -------------------------------------------------------
# TABLE DEFINITIONS
# Each entry maps to one Iceberg table:
#   - table_name   : name in Glue catalog
#   - partition_by : column used for partitioning
#   - ddl_columns  : exact DDL string for CREATE TABLE
#   - select_cols  : columns to SELECT for INSERT
# -------------------------------------------------------
ICEBERG_TABLES = [
    {
        "table_name":   "iceberg_customer",
        "partition_by": "customer_id",
        "ddl_columns": """
            customer_id          int,
            customer_first_name  string,
            customer_last_name   string,
            customer_email       string,
            customer_phone       string,
            email_hashed         string
        """,
        "select_cols": [
            "customer_id", "customer_first_name", "customer_last_name",
            "customer_email", "customer_phone", "email_hashed"
        ]
    },
    {
        "table_name":   "iceberg_address",
        "partition_by": "customer_id",
        "ddl_columns": """
            customer_id      int,
            customer_street  string,
            customer_city    string,
            customer_state   string,
            customer_zip     string
        """,
        "select_cols": [
            "customer_id", "customer_street", "customer_city",
            "customer_state", "customer_zip"
        ]
    },
    {
        "table_name":   "iceberg_orders",
        "partition_by": "order_date",
        "ddl_columns": """
            order_id      int,
            customer_id   int,
            order_date    date,
            order_amount  double,
            order_status  string,
            total_amount  double
        """,
        "select_cols": [
            "order_id", "customer_id", "order_date",
            "order_amount", "order_status", "total_amount"
        ]
    },
    {
        "table_name":   "iceberg_payments",
        "partition_by": "payment_date",
        "ddl_columns": """
            payment_id      int,
            order_id        int,
            payment_date    date,
            amount          double,
            payment_method  string
        """,
        "select_cols": [
            "payment_id", "order_id", "payment_date",
            "amount", "payment_method"
        ]
    }
]

# -------------------------------------------------------
# LOOP — CREATE + INSERT each of the 4 Iceberg tables
# -------------------------------------------------------
for tbl in ICEBERG_TABLES:
    tbl_name      = tbl["table_name"]
    partition_col = tbl["partition_by"]
    ddl_cols      = tbl["ddl_columns"]
    select_cols   = tbl["select_cols"]
    full_ref      = f"glue_catalog.{database}.{tbl_name}"
    location      = f"{warehouse}/final/{tbl_name}"
    select_str    = ", ".join(select_cols)

    logger.info(f"[Iceberg Load] Processing table: {full_ref}")

    # -- Deduplicate this table's slice before loading
    # customer and address: deduplicate on their own primary key
    # orders: deduplicate on order_id
    # payments: deduplicate on payment_id
    pk_map = {
        "iceberg_customer": "customer_id",
        "iceberg_address":  "customer_id",
        "iceberg_orders":   "order_id",
        "iceberg_payments": "payment_id"
    }
    pk = pk_map[tbl_name]

    df_slice = df.select(*select_cols).dropDuplicates([pk])
    slice_count = df_slice.count()
    logger.info(f"[Iceberg Load] {tbl_name} — rows after dedup: {slice_count}")

    # Register slice as its own temp view for SQL INSERT
    view_name = f"view_{tbl_name}"
    df_slice.createOrReplaceTempView(view_name)

    # -- CREATE TABLE IF NOT EXISTS
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_ref} (
            {ddl_cols}
        )
        USING iceberg
        PARTITIONED BY ({partition_col})
        LOCATION '{location}'
        TBLPROPERTIES (
            'table_type'                      = 'ICEBERG',
            'format'                          = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """
    spark.sql(create_sql)
    logger.info(f"[Iceberg Load] Table ready: {full_ref}")

    # -- INSERT DATA
    insert_sql = f"""
        INSERT INTO {full_ref}
        SELECT {select_str}
        FROM {view_name}
    """
    spark.sql(insert_sql)

    # -- VERIFY
    verify_count = spark.sql(
        f"SELECT COUNT(*) as cnt FROM {full_ref}"
    ).collect()[0]["cnt"]

    logger.info(f"[Iceberg Load] {tbl_name} — rows loaded: {verify_count} | location: {location}")

# -------------------------------------------------------
# DONE
# -------------------------------------------------------
logger.info(f"[Iceberg Load] All 4 Iceberg tables loaded successfully")
logger.info(f"[Iceberg Load] Database: {database}")
logger.info(f"[Iceberg Load] Tables: iceberg_customer, iceberg_address, iceberg_orders, iceberg_payments")
logger.info(f"[Iceberg Load] Job completed at {datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")

job.commit()