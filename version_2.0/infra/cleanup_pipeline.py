import boto3
import json
import time
import os

# -------------------------------------------------------
# CONFIG LOAD
# -------------------------------------------------------
CONFIG_DIR = os.path.join(os.path.dirname(__file__), "../config")

with open(os.path.join(CONFIG_DIR, "infra_config.json")) as f:
    infra_cfg = json.load(f)

with open(os.path.join(CONFIG_DIR, "dms_config.json")) as f:
    dms_cfg = json.load(f)

with open(os.path.join(CONFIG_DIR, "glue_config.json")) as f:
    glue_cfg = json.load(f)

region     = infra_cfg["aws"]["region"]
account_id = infra_cfg["aws"]["account_id"]

# Clients
glue           = boto3.client("glue",           region_name=region)
dms            = boto3.client("dms",            region_name=region)
rds            = boto3.client("rds",            region_name=region)
iam            = boto3.client("iam")
secretsmanager = boto3.client("secretsmanager", region_name=region)
ec2            = boto3.client("ec2",            region_name=region)
s3_resource    = boto3.resource("s3",           region_name=region)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

# -------------------------------------------------------
# CLEANUP STEPS
# -------------------------------------------------------

def cleanup_glue_workflow():
    wf_name = "etl-pipeline-v2.1"
    log(f"CLEANUP: Glue Workflow '{wf_name}'")
    
    try:
        # 1. Delete Triggers first
        response = glue.get_workflow(Name=wf_name, IncludeGraph=True)
        graph = response["Workflow"].get("Graph", {})
        for node in graph.get("Nodes", []):
            if node["Type"] == "TRIGGER":
                trigger_name = node["Name"]
                glue.delete_trigger(Name=trigger_name)
                log(f"  Deleted trigger: {trigger_name}")
        
        # 2. Delete Workflow
        glue.delete_workflow(Name=wf_name)
        log(f"  Deleted workflow: {wf_name}")
    except glue.exceptions.EntityNotFoundException:
        log(f"  Workflow {wf_name} not found — skipping")


def cleanup_glue_jobs():
    log("CLEANUP: Glue Jobs")
    job_names = [
        "job_1_profiling", "job_2_deduplication", "job_3_pii_masking",
        "job_4_etl", "job_5_dq_check", "job_6_iceberg_load"
    ]
    for name in job_names:
        try:
            glue.delete_job(JobName=name)
            log(f"  Deleted job: {name}")
        except glue.exceptions.EntityNotFoundException:
            pass


def cleanup_glue_database():
    db_name = glue_cfg["iceberg"]["database"]
    log(f"CLEANUP: Glue Database '{db_name}'")
    try:
        glue.delete_database(Name=db_name)
        log(f"  Deleted database: {db_name}")
    except glue.exceptions.EntityNotFoundException:
        log(f"  Database not found — skipping")


def cleanup_s3():
    bucket_name = infra_cfg["s3"]["bucket"]
    log(f"CLEANUP: S3 Bucket '{bucket_name}'")
    bucket = s3_resource.Bucket(bucket_name)
    try:
        log("  Deleting all objects in the bucket (this may take a moment)...")
        bucket.objects.all().delete()
        log("  All objects wiped out successfully.")
        
        log("  Deleting the bucket itself...")
        bucket.delete()
        log(f"  Deleted bucket: {bucket_name}")
    except Exception as e:
        if "NoSuchBucket" in str(e) or "Not Found" in str(e):
            log("  Bucket not found — skipping")
        else:
            log(f"  Warning during S3 cleanup: {e}")


def cleanup_dms():
    log("CLEANUP: DMS Resources")
    task_id = dms_cfg["dms"]["task_id"]
    
    # 1. Delete Task
    try:
        tasks = dms.describe_replication_tasks(Filters=[{"Name":"replication-task-id", "Values":[task_id]}])["ReplicationTasks"]
        if tasks:
            task_arn = tasks[0]["ReplicationTaskArn"]
            status   = tasks[0]["Status"]
            if status in ("running", "starting", "stopped", "failed"):
                if status != "stopped":
                    dms.stop_replication_task(ReplicationTaskArn=task_arn)
                    log("  Stopping DMS Task...")
                    while True:
                        t = dms.describe_replication_tasks(Filters=[{"Name":"replication-task-id", "Values":[task_id]}])["ReplicationTasks"][0]
                        if t["Status"] == "stopped": 
                            break
                        log(f"  Waiting for DMS Task to stop (Current Status: {t['Status']})...")
                        time.sleep(10)
                dms.delete_replication_task(ReplicationTaskArn=task_arn)
                log(f"  Deleted DMS task: {task_id}")
    except Exception as e:
        log(f"  Task cleanup note: {str(e)[:50]}")

    # 2. Delete Endpoints
    for eid in [dms_cfg["endpoints"]["source_id"], dms_cfg["endpoints"]["target_id"]]:
        try:
            eps = dms.describe_endpoints(Filters=[{"Name":"endpoint-id", "Values":[eid]}])["Endpoints"]
            if eps:
                dms.delete_endpoint(EndpointArn=eps[0]["EndpointArn"])
                log(f"  Deleted endpoint: {eid}")
        except Exception: pass

    # 3. Delete Instance with Waiter
    inst_id = dms_cfg["dms"]["replication_instance_id"]
    try:
        instances = dms.describe_replication_instances(Filters=[{"Name":"replication-instance-id", "Values":[inst_id]}])["ReplicationInstances"]
        if instances:
            dms.delete_replication_instance(ReplicationInstanceArn=instances[0]["ReplicationInstanceArn"])
            log(f"  Deleting replication instance: {inst_id}...")
            
            while True:
                time.sleep(15)
                res = dms.describe_replication_instances(Filters=[{"Name":"replication-instance-id", "Values":[inst_id]}])["ReplicationInstances"]
                if not res:
                    break
                
                status = res[0].get("ReplicationInstanceStatus", "deleting")
                log(f"  Waiting for DMS instance to be deleted (Status: {status})...")
                
            log("  Replication instance successfully deleted!")
    except Exception as e: 
        if "ResourceNotFoundFault" not in str(e):
            log(f"  Replication instance cleanup note: {e}")


def cleanup_rds():
    db_id = infra_cfg["rds"]["identifier"]
    log(f"CLEANUP: RDS Instance '{db_id}'")
    try:
        instances = rds.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"]
        if instances:
            status = instances[0]["DBInstanceStatus"]
            if status != "deleting":
                rds.delete_db_instance(
                    DBInstanceIdentifier=db_id,
                    SkipFinalSnapshot=True
                )
                log(f"  Instance deletion initiated (SkipFinalSnapshot=True).")

            log("  Waiting for RDS instance to be fully deleted... (This may take ~5-10 minutes)")
            waiter = rds.get_waiter("db_instance_deleted")
            waiter.wait(DBInstanceIdentifier=db_id)
            log("  RDS instance successfully deleted!")
    except rds.exceptions.DBInstanceNotFoundFault:
        log("  RDS instance not found — skipping")
    except Exception as e:
        log(f"  RDS cleanup note: {e}")
    
    # Cleanup SG rule
    try:
        sg_id = infra_cfg["network"]["security_group_id"]
        ec2.revoke_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432, 'IpRanges': [{'CidrIp': '172.31.0.0/16'}]}]
        )
        log("  Revoked DMS security group rule")
    except Exception: pass


def cleanup_iam():
    role_name = infra_cfg["iam"]["role_name"]
    log(f"CLEANUP: IAM Role '{role_name}'")
    try:
        # Remove Inline Policies
        policies = iam.list_role_policies(RoleName=role_name)["PolicyNames"]
        for p in policies:
            iam.delete_role_policy(RoleName=role_name, PolicyName=p)
            log(f"  Removed inline policy: {p}")
        
        # Delete Role
        iam.delete_role(RoleName=role_name)
        log(f"  Deleted IAM role: {role_name}")
    except iam.exceptions.NoSuchEntityException:
        log("  IAM Role not found — skipping")


def cleanup_secrets():
    secret_name = infra_cfg["secrets"]["db_secret_name"]
    log(f"CLEANUP: Secrets Manager '{secret_name}'")
    try:
        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
        log(f"  Deleted secret: {secret_name}")
    except secretsmanager.exceptions.ResourceNotFoundException:
        log("  Secret not found — skipping")


def main():
    print("\n" + "="*65)
    print("  AWS Pipeline VERSION 2.0 — INFRASTRUCTURE CLEANUP")
    print("="*65)
    
    cleanup_glue_workflow()
    cleanup_glue_jobs()
    cleanup_glue_database()
    cleanup_dms()
    cleanup_rds()
    cleanup_s3()
    cleanup_iam()
    cleanup_secrets()
    
    print("\n" + "="*65)
    print("  CLEANUP COMPLETED SUCCESSFULLY")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()
