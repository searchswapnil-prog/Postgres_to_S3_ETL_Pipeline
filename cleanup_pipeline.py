"""
cleanup_pipeline.py
───────────────────
Run this at the end of the day to delete all expensive AWS infrastructure.
It deletes DMS, RDS, and empties/deletes the S3 bucket.
"""

import boto3
import json
import time

with open("config/infra_config.json") as f:
    config = json.load(f)

region = config["aws"]["region"]
dms = boto3.client("dms", region_name=region)
rds = boto3.client("rds", region_name=region)
s3_resource = boto3.resource("s3", region_name=region)
s3_client = boto3.client("s3", region_name=region)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def cleanup():
    print("\n" + "=" * 55)
    print("  AWS Pipeline Infrastructure Cleanup")
    print("=" * 55 + "\n")

    cfg_dms = config["dms"]
    cfg_rds = config["rds"]
    bucket_name = config["s3"]["bucket"]

    # 1. Delete DMS Task
    log("STEP 1 — Deleting DMS Task")
    try:
        tasks = dms.describe_replication_tasks(Filters=[{"Name": "replication-task-id", "Values": [cfg_dms["task_id"]]}])["ReplicationTasks"]
        if tasks:
            task_arn = tasks[0]["ReplicationTaskArn"]
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
            log("  DMS Task deleted.")
    except Exception as e:
        log(f"  Skipped DMS Task: {str(e)}")

    # 2. Delete DMS Endpoints
    log("STEP 2 — Deleting DMS Endpoints")
    try:
        endpoints = dms.describe_endpoints()["Endpoints"]
        for ep in endpoints:
            if ep["EndpointIdentifier"] in [config["endpoints"]["source_id"], config["endpoints"]["target_id"]]:
                dms.delete_endpoint(EndpointArn=ep["EndpointArn"])
                log(f"  Deleted endpoint: {ep['EndpointIdentifier']}")
    except Exception as e:
        log(f"  Skipped DMS Endpoints: {str(e)}")

    # 3. Delete DMS Replication Instance
    log("STEP 3 — Deleting DMS Replication Instance")
    try:
        instances = dms.describe_replication_instances(Filters=[{"Name": "replication-instance-id", "Values": [cfg_dms["replication_instance_id"]]}])["ReplicationInstances"]
        if instances:
            instance_arn = instances[0]["ReplicationInstanceArn"]
            dms.delete_replication_instance(ReplicationInstanceArn=instance_arn)
            log("  DMS Replication Instance deletion started.")
    except Exception as e:
        log(f"  Skipped Replication Instance: {str(e)}")

    # 4. Delete RDS Database
    log("STEP 4 — Deleting RDS Database (No Final Snapshot)")
    try:
        rds.delete_db_instance(
            DBInstanceIdentifier=cfg_rds["identifier"],
            SkipFinalSnapshot=True # Skips creating a new snapshot to save money
        )
        log("  RDS Database deletion started.")
    except Exception as e:
        log(f"  Skipped RDS Database: {str(e)}")

    # 5. Empty and Delete S3 Bucket
    log("STEP 5 — Emptying and Deleting S3 Bucket")
    try:
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()
        log("  S3 Bucket emptied.")
        s3_client.delete_bucket(Bucket=bucket_name)
        log("  S3 Bucket deleted.")
    except Exception as e:
        log(f"  Skipped S3 Bucket: {str(e)}")

    print("\n" + "=" * 55)
    print("  Cleanup Commands Issued. Infrastructure is shutting down.")
    print("=" * 55 + "\n")

if __name__ == "__main__":
    cleanup()