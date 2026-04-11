"""
infra/setup_s3.py
─────────────────
Manages the S3 bucket used by the entire pipeline:
  - Creates the bucket (ap-south-1) if it doesn't exist
  - Scaffolds all required folder prefixes
  - Uploads glue_config.json and all Glue job scripts from glue_jobs/
"""

import boto3
import json
import os
import time

with open("config/infra_config.json") as f:
    config = json.load(f)

region    = config["aws"]["region"]
BUCKET    = config["s3"]["bucket"]

s3_client = boto3.client("s3", region_name=region)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


# ─────────────────────────────────────────────
# STEP 1 — Bucket + folder structure
# ─────────────────────────────────────────────

def create_s3_bucket():
    log("STEP 1 — Setting up S3 bucket and folder structure")

    try:
        s3_client.head_bucket(Bucket=BUCKET)
        log(f"  Bucket already exists: {BUCKET}")
    except Exception:
        try:
            s3_client.create_bucket(
                Bucket=BUCKET,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
            log(f"  Created bucket: {BUCKET}")
        except Exception as e:
            log(f"  Bucket creation note: {str(e)}")

    folders = [
        "raw/public/",
        "staging/deduplicated/",
        "staging/masked/",
        "processed/",
        "iceberg/",
        "error/",
        "logs/profiling/",
        "logs/dq/",
        "config/",
        "scripts/",
    ]
    for folder in folders:
        s3_client.put_object(Bucket=BUCKET, Key=folder)

    log("  S3 folder structure created")


# ─────────────────────────────────────────────
# STEP 2 — Upload config + Glue scripts
# ─────────────────────────────────────────────

def upload_scripts():
    """
    Uploads glue_config.json from config/ and all job_*.py scripts
    from glue_jobs/ to their respective S3 prefixes.

    Local layout expected:
        config/glue_config.json
        glue_jobs/job_1_profiling.py
        glue_jobs/job_2_deduplication.py
        glue_jobs/job_3_pii_masking.py
        glue_jobs/job_4_etl.py
        glue_jobs/job_5_dq_check.py
        glue_jobs/job_6_iceberg_load.py
    """
    log("STEP 2 — Uploading config and Glue job scripts to S3")

    # Config
    config_local = os.path.join("config", "glue_config.json")
    if os.path.exists(config_local):
        s3_client.upload_file(config_local, BUCKET, "config/glue_config.json")
        log("  Uploaded: config/glue_config.json")
    else:
        log(f"  WARNING: {config_local} not found locally — skipping")

    # Glue job scripts
    scripts = [
        "job_1_profiling.py",
        "job_2_deduplication.py",
        "job_3_pii_masking.py",
        "job_4_etl.py",
        "job_5_dq_check.py",
        "job_6_iceberg_load.py",
    ]

    for script in scripts:
        local_path = os.path.join("glue_jobs", script)
        if os.path.exists(local_path):
            s3_client.upload_file(local_path, BUCKET, f"scripts/{script}")
            log(f"  Uploaded: glue_jobs/{script} → s3://{BUCKET}/scripts/{script}")
        else:
            log(f"  WARNING: {local_path} not found locally — skipping")

    log("  Upload step complete")
