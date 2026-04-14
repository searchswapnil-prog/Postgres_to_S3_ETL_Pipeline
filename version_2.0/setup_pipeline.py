"""
setup_pipeline.py
─────────────────
Master infrastructure setup runner.
Run this once (or re-run safely — all steps are idempotent) to provision:
  - RDS PostgreSQL + Secrets Manager + Security Group rules
  - S3 bucket + folder structure
  - IAM role (shared by DMS and Glue)
  - DMS replication instance, endpoints, and task

Usage (from the project root):
    python setup_pipeline.py

All AWS resources are idempotent — existing resources are detected
and skipped, never recreated.
"""

import sys
import os

# Make infra/ importable from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "infra"))

import setup_rds
import setup_iam
import setup_s3
import setup_dms


def main():
    print("\n" + "=" * 55)
    print("  AWS Pipeline Infrastructure Setup")
    print("=" * 55 + "\n")

    # ── Secrets + DB credentials ──────────────────────────
    setup_rds.manage_db_credentials()           # STEP 0   — retrieve/create password

    # ── Network / Security Group ──────────────────────────
    setup_rds.whitelist_current_ip()            # STEP 0.2 — open 5432 for your machine
    setup_rds.allow_dms_in_security_group()     # STEP 0.3 — open 5432 for DMS VPC CIDR

    # ── RDS ───────────────────────────────────────────────
    setup_rds.create_rds_database()             # STEP 0.5 — provision RDS, capture endpoint
    setup_rds.enable_logical_replication()      # STEP 0.4 — parameter group + reboot

    # ── S3 ────────────────────────────────────────────────
    setup_s3.create_s3_bucket()                 # STEP 1   — bucket + folder scaffold
    setup_s3.upload_scripts()                   # STEP 2   — config + Glue job scripts

    # ── IAM ───────────────────────────────────────────────
    setup_iam.create_iam_role()                 # STEP 3   — role + trust + inline policies

    # ── DMS ───────────────────────────────────────────────
    setup_dms.create_replication_instance()     # STEP 4   — replication instance
    setup_dms.create_source_endpoint(           # STEP 5   — PostgreSQL source
        db_password = setup_rds.DYNAMIC_DB_PASSWORD,
        db_endpoint = setup_rds.DYNAMIC_DB_ENDPOINT,
    )
    setup_dms.create_target_endpoint()          # STEP 6   — S3 target
    setup_dms.test_endpoints()                  # STEP 7   — connection tests
    setup_dms.create_task()                     # STEP 8   — replication task
    setup_dms.reload_dms_task()                 # STEP 9   — fresh full-load

    # ── Summary ───────────────────────────────────────────
    # ── Glue Workflow ─────────────────────────────────────
    print("\n  Setting up Glue Workflow via IAC...")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "glue_workflow", "src"))
    try:
        import create_workflow
        create_workflow.create_workflow()
    except Exception as e:
        print(f"Failed to create glue workflow: {e}")

    import json
    with open("config/infra_config.json") as f:
        config = json.load(f)

    print("\n" + "=" * 55)
    print("  Setup Complete!")
    print(f"  DBeaver Host  : {setup_rds.DYNAMIC_DB_ENDPOINT}")
    print(f"  Port          : 5432")
    print(f"  Database      : {config['rds']['database']}")
    print(f"  Username      : {config['rds']['username']}")
    print(f"  Password from : AWS Secrets Manager → {config['secrets']['db_secret_name']}")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()
