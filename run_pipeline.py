"""
run_pipeline.py
───────────────
AWS Glue Pipeline Orchestrator — Full Automated Execution.

Run this locally from your VS Code terminal (from the project root):
    python run_pipeline.py

Prerequisites:
    pip install boto3
    AWS CLI configured (aws configure) with valid credentials
    setup_pipeline.py has been run at least once

What this script does:
    1. Ensures IAM trust + Glue catalog policies are current (idempotent)
    2. Creates/updates all 6 Glue jobs with correct Iceberg configuration
    3. Runs jobs sequentially, passing timestamps between stages
    4. Reports DQ score and final data locations on completion
"""

import boto3
import json
import time
import sys
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG — loaded from config/ folder
# ─────────────────────────────────────────────

with open("config/infra_config.json") as f:
    _infra = json.load(f)

REGION       = _infra["aws"]["region"]
ACCOUNT_ID   = _infra["aws"]["account_id"]
BUCKET       = _infra["s3"]["bucket"]
IAM_ROLE     = f"arn:aws:iam::{ACCOUNT_ID}:role/{_infra['iam']['role_name']}"
IAM_ROLE_NAME= _infra["iam"]["role_name"]

CONFIG_S3    = f"s3://{BUCKET}/config/glue_config.json"
SCRIPTS_BASE = f"s3://{BUCKET}/scripts"

GLUE_VERSION = "4.0"
WORKER_TYPE  = "G.1X"
NUM_WORKERS  = 2

# Iceberg Spark config — all three must be passed together in --conf
ICEBERG_CONF = (
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    " --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog"
    " --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
    f" --conf spark.sql.catalog.glue_catalog.warehouse=s3://{BUCKET}/iceberg"
)

COMMON_JOB_ARGS = {
    "--conf":                    ICEBERG_CONF,
    "--datalake-formats":        "iceberg",
    "--enable-glue-datacatalog": "true"
}

# ─────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────

glue = boto3.client("glue", region_name=REGION)
s3   = boto3.client("s3",   region_name=REGION)
iam  = boto3.client("iam")


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def log(msg, level="INFO"):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    prefix = {
        "INFO":    "✅",
        "WAIT":    "⏳",
        "ERROR":   "❌",
        "SECTION": "═" * 55,
    }.get(level, "  ")
    if level == "SECTION":
        print(f"\n{prefix}")
        print(f"  {msg}")
        print(prefix)
    else:
        print(f"[{ts}] {prefix}  {msg}")


# ─────────────────────────────────────────────
# STEP 0 — IAM permissions (idempotent)
# ─────────────────────────────────────────────

def step_ensure_iam():
    log("STEP 0 — Ensuring IAM Permissions", "SECTION")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "dms.amazonaws.com"},  "Action": "sts:AssumeRole"},
            {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}
        ]
    }

    iam.update_assume_role_policy(
        RoleName       = IAM_ROLE_NAME,
        PolicyDocument = json.dumps(trust_policy)
    )
    log(f"Trust policy set — DMS + Glue can assume: {IAM_ROLE_NAME}")

    glue_catalog_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase", "glue:CreateDatabase",
                    "glue:GetTable",    "glue:CreateTable",
                    "glue:UpdateTable", "glue:DeleteTable",
                    "glue:GetPartition","glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:GetPartitions","glue:BatchGetPartition"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/glue_iceberg_db",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/glue_iceberg_db/*"
                ]
            }
        ]
    }

    iam.put_role_policy(
        RoleName       = IAM_ROLE_NAME,
        PolicyName     = "glue-catalog-iceberg-policy",
        PolicyDocument = json.dumps(glue_catalog_policy)
    )
    log(f"Inline policy 'glue-catalog-iceberg-policy' applied to: {IAM_ROLE_NAME}")

    log("Waiting 10s for IAM propagation...", "WAIT")
    time.sleep(10)
    log("IAM ready")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def create_glue_job(job_name, script_filename, extra_args=None):
    """Create or update a Glue job with correct Iceberg DefaultArguments."""
    default_args = {**COMMON_JOB_ARGS}
    if extra_args:
        default_args.update(extra_args)

    job_config = {
        "Role": IAM_ROLE,
        "Command": {
            "Name":           "glueetl",
            "ScriptLocation": f"{SCRIPTS_BASE}/{script_filename}",
            "PythonVersion":  "3"
        },
        "GlueVersion":      GLUE_VERSION,
        "WorkerType":       WORKER_TYPE,
        "NumberOfWorkers":  NUM_WORKERS,
        "DefaultArguments": default_args
    }

    try:
        glue.get_job(JobName=job_name)
        glue.update_job(JobName=job_name, JobUpdate=job_config)
        log(f"Updated existing job: {job_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=job_name, **job_config)
        log(f"Created new job: {job_name}")


def start_job(job_name, arguments):
    response = glue.start_job_run(JobName=job_name, Arguments=arguments)
    run_id   = response["JobRunId"]
    log(f"Started [{job_name}] → Run ID: {run_id}")
    return run_id


def wait_for_job(job_name, run_id, poll_seconds=30):
    log(f"Waiting for [{job_name}] to complete...", "WAIT")
    terminal_states = {"SUCCEEDED", "FAILED", "ERROR", "TIMEOUT", "STOPPED"}

    while True:
        response = glue.get_job_run(JobName=job_name, RunId=run_id)
        state    = response["JobRun"]["JobRunState"]

        if state in terminal_states:
            if state == "SUCCEEDED":
                log(f"[{job_name}] finished → {state}")
                return state
            else:
                error = response["JobRun"].get("ErrorMessage", "No error message")
                # sys.exit(0) inside a Glue job shows as FAILED — means intentional skip
                if "SystemExit: 0" in error:
                    log(f"[{job_name}] intentionally skipped (DQ FAIL gate) → treated as OK")
                    return "SKIPPED"
                log(f"[{job_name}] failed with state: {state}", "ERROR")
                log(f"Error: {error}", "ERROR")
                sys.exit(1)

        log(f"[{job_name}] current state: {state} — checking again in {poll_seconds}s...", "WAIT")
        time.sleep(poll_seconds)


def get_latest_s3_folder(prefix):
    """Return the most recent timestamp-named folder under an S3 prefix."""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter="/")
    folders  = [
        cp["Prefix"].replace(prefix, "").rstrip("/")
        for cp in response.get("CommonPrefixes", [])
        if cp["Prefix"].replace(prefix, "").rstrip("/")
    ]

    if not folders:
        log(f"No folders found under s3://{BUCKET}/{prefix}", "ERROR")
        sys.exit(1)

    latest = sorted(folders)[-1]
    log(f"Latest output folder: {latest}")
    return latest


def get_dq_report(etl_run_ts):
    """Download and parse the DQ JSON report from S3 logs."""
    prefix   = "logs/dq/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    report_key = None

    for obj in response.get("Contents", []):
        if etl_run_ts in obj["Key"]:
            report_key = obj["Key"]
            break

    if not report_key:
        all_reports = [
            o["Key"] for o in response.get("Contents", [])
            if o["Key"].endswith(".json")
        ]
        if not all_reports:
            log("No DQ report found in S3!", "ERROR")
            sys.exit(1)
        report_key = sorted(all_reports)[-1]

    obj    = s3.get_object(Bucket=BUCKET, Key=report_key)
    report = json.loads(obj["Body"].read().decode("utf-8"))

    log(f"DQ Report : s3://{BUCKET}/{report_key}")
    log(f"DQ Score  : {report['dq_score_pct']}%")
    log(f"DQ Status : {report['dq_status']}")
    log(f"Checks    : {report['passed_checks']}/{report['total_checks']} passed")

    if report.get("failed_checks"):
        log("Failed checks:", "WAIT")
        for fc in report["failed_checks"]:
            print(f"         → [{fc['check']}] column: {fc['column']} | failed rows: {fc['failed_count']}")

    return report["run_timestamp"], report["dq_status"]


# ─────────────────────────────────────────────
# PIPELINE STEPS
# ─────────────────────────────────────────────

def step_create_jobs():
    log("STEP 1 — Creating / Updating All 6 Glue Jobs", "SECTION")
    jobs = [
        ("job_1_profiling",     "job_1_profiling.py"),
        ("job_2_deduplication", "job_2_deduplication.py"),
        ("job_3_pii_masking",   "job_3_pii_masking.py"),
        ("job_4_etl",           "job_4_etl.py"),
        ("job_5_dq_check",      "job_5_dq_check.py"),
        ("job_6_iceberg_load",  "job_6_iceberg_load.py"),
    ]
    for job_name, script in jobs:
        create_glue_job(job_name, script)
    log("All 6 jobs are ready in AWS Glue")


def step_run_job1():
    log("STEP 2 — Running Job 1: Data Profiling", "SECTION")
    run_id = start_job("job_1_profiling", {"--config_s3_path": CONFIG_S3})
    wait_for_job("job_1_profiling", run_id)
    log(f"Profiling report → s3://{BUCKET}/logs/profiling/")


def step_run_job2():
    log("STEP 3 — Running Job 2: Deduplication", "SECTION")
    run_id = start_job("job_2_deduplication", {"--config_s3_path": CONFIG_S3})
    wait_for_job("job_2_deduplication", run_id)
    dedup_run_ts = get_latest_s3_folder("staging/deduplicated/customer/")
    log(f"Dedup output timestamp: {dedup_run_ts}")
    return dedup_run_ts


def step_run_job3(dedup_run_ts):
    log("STEP 4 — Running Job 3: PII Masking", "SECTION")
    log(f"Using dedup_run_ts = {dedup_run_ts}")
    run_id = start_job(
        "job_3_pii_masking",
        {"--config_s3_path": CONFIG_S3, "--dedup_run_ts": dedup_run_ts}
    )
    wait_for_job("job_3_pii_masking", run_id)
    masked_run_ts = get_latest_s3_folder("staging/masked/customer/")
    log(f"Masked output timestamp: {masked_run_ts}")
    return masked_run_ts


def step_run_job4(masked_run_ts):
    log("STEP 5 — Running Job 4: ETL", "SECTION")
    log(f"Using masked_run_ts = {masked_run_ts}")
    run_id = start_job(
        "job_4_etl",
        {"--config_s3_path": CONFIG_S3, "--masked_run_ts": masked_run_ts}
    )
    wait_for_job("job_4_etl", run_id)
    etl_run_ts = get_latest_s3_folder("processed/")
    log(f"ETL output timestamp: {etl_run_ts}")
    return etl_run_ts


def step_run_job5(etl_run_ts):
    log("STEP 6 — Running Job 5: DQ Check", "SECTION")
    log(f"Using etl_run_ts = {etl_run_ts}")
    run_id = start_job(
        "job_5_dq_check",
        {"--config_s3_path": CONFIG_S3, "--etl_run_ts": etl_run_ts}
    )
    wait_for_job("job_5_dq_check", run_id)
    return get_dq_report(etl_run_ts)


def step_run_job6(dq_run_ts, dq_status):
    log("STEP 7 — Running Job 6: Iceberg Load", "SECTION")
    log(f"Using dq_run_ts = {dq_run_ts} | dq_status = {dq_status}")

    if dq_status != "PASS":
        log(f"DQ status is {dq_status} — Job 6 will safely skip the load.", "WAIT")
        log(f"Rejected data → s3://{BUCKET}/error/")

    run_id = start_job(
        "job_6_iceberg_load",
        {"--config_s3_path": CONFIG_S3, "--dq_run_ts": dq_run_ts, "--dq_status": dq_status}
    )
    wait_for_job("job_6_iceberg_load", run_id)

    if dq_status == "PASS":
        log(f"Final data → s3://{BUCKET}/iceberg/final/ (4 tables)")
        log("Tables registered: iceberg_customer, iceberg_address, iceberg_orders, iceberg_payments")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print()
    log("AWS Glue Pipeline — Full Automated Run", "SECTION")
    log(f"Bucket : {BUCKET}")
    log(f"Region : {REGION}")
    log(f"Config : {CONFIG_S3}")
    print()

    pipeline_start = time.time()

    step_ensure_iam()
    step_create_jobs()
    step_run_job1()
    dedup_run_ts  = step_run_job2()
    masked_run_ts = step_run_job3(dedup_run_ts)
    etl_run_ts    = step_run_job4(masked_run_ts)
    dq_run_ts, dq_status = step_run_job5(etl_run_ts)
    step_run_job6(dq_run_ts, dq_status)

    elapsed = round((time.time() - pipeline_start) / 60, 1)
    log("PIPELINE COMPLETE", "SECTION")
    log(f"Total time  : {elapsed} minutes")
    log(f"Dedup ts    : {dedup_run_ts}")
    log(f"Masked ts   : {masked_run_ts}")
    log(f"ETL ts      : {etl_run_ts}")
    log(f"DQ ts       : {dq_run_ts}")
    log(f"DQ status   : {dq_status}")
    log(f"Profiling   : s3://{BUCKET}/logs/profiling/")
    log(f"DQ report   : s3://{BUCKET}/logs/dq/")
    log(f"Final data  : s3://{BUCKET}/iceberg/final/ (4 tables)")
    print()


if __name__ == "__main__":
    main()
