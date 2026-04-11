"""
infra/setup_rds.py
──────────────────
Handles everything related to the RDS PostgreSQL database:
  - Secrets Manager: create/retrieve DB password
  - Security Group: whitelist your machine IP and DMS VPC CIDR
  - RDS instance: provision and wait for availability
  - Logical replication: parameter group + reboot + in-sync verification
"""

import boto3
import json
import time
import string
import secrets
import urllib.request

with open("config/infra_config.json") as f:
    config = json.load(f)

region = config["aws"]["region"]

rds            = boto3.client("rds",            region_name=region)
ec2            = boto3.client("ec2",            region_name=region)
secretsmanager = boto3.client("secretsmanager", region_name=region)

DYNAMIC_DB_PASSWORD = ""
DYNAMIC_DB_ENDPOINT = ""


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


# ─────────────────────────────────────────────
# HELPERS — Security Group
# ─────────────────────────────────────────────

def _get_existing_port_5432_cidrs(sg_id):
    sg = ec2.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
    return [
        ip_range["CidrIp"]
        for rule in sg["IpPermissions"]
        if rule.get("FromPort") == 5432 and rule.get("IpProtocol") == "tcp"
        for ip_range in rule.get("IpRanges", [])
    ]


def _add_sg_rule(sg_id, cidr, description):
    existing = _get_existing_port_5432_cidrs(sg_id)
    if cidr in existing:
        log(f"  {cidr} already whitelisted on port 5432 — skipping")
        return
    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[{
            "IpProtocol": "tcp",
            "FromPort":   5432,
            "ToPort":     5432,
            "IpRanges":   [{"CidrIp": cidr, "Description": description}]
        }]
    )
    log(f"  Rule added: {cidr} -> port 5432")


# ─────────────────────────────────────────────
# STEP 0 — Secrets Manager
# ─────────────────────────────────────────────

def manage_db_credentials():
    log("STEP 0 — Managing DB Credentials via Secrets Manager")
    secret_name = config["secrets"]["db_secret_name"]
    username    = config["rds"]["username"]

    global DYNAMIC_DB_PASSWORD

    try:
        response    = secretsmanager.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response["SecretString"])
        DYNAMIC_DB_PASSWORD = secret_data["password"]
        log("  Retrieved existing database password from Secrets Manager.")
    except secretsmanager.exceptions.ResourceNotFoundException:
        log("  Secret not found. Generating new secure password...")
        alphabet     = string.ascii_letters + string.digits
        new_password = "".join(secrets.choice(alphabet) for _ in range(16))
        secretsmanager.create_secret(
            Name=secret_name,
            Description="Auto-generated credentials for RDS pipeline database",
            SecretString=json.dumps({"username": username, "password": new_password})
        )
        DYNAMIC_DB_PASSWORD = new_password
        log("  New password generated and stored in Secrets Manager.")


# ─────────────────────────────────────────────
# STEP 0.2 — Whitelist local machine IP (DBeaver)
# ─────────────────────────────────────────────

def whitelist_current_ip():
    log("STEP 0.2 — Whitelisting current machine IP for DBeaver access")
    sg_id     = config["network"]["security_group_id"]
    public_ip = urllib.request.urlopen(
        "https://checkip.amazonaws.com"
    ).read().decode("utf-8").strip()
    cidr = f"{public_ip}/32"
    log(f"  Detected public IP: {cidr}")
    _add_sg_rule(sg_id, cidr, f"DBeaver access auto-added {time.strftime('%Y-%m-%d')}")


# ─────────────────────────────────────────────
# STEP 0.3 — Whitelist VPC CIDR (DMS → RDS)
# ─────────────────────────────────────────────

def allow_dms_in_security_group():
    log("STEP 0.3 — Allowing DMS replication instance to reach RDS via VPC CIDR")
    sg_id    = config["network"]["security_group_id"]
    vpc_cidr = "172.31.0.0/16"
    _add_sg_rule(sg_id, vpc_cidr, "DMS replication instance VPC internal access")


# ─────────────────────────────────────────────
# STEP 0.4 — Logical replication parameter group
# ─────────────────────────────────────────────

def enable_logical_replication():
    """
    Creates/verifies the RDS parameter group with rds.logical_replication=1.
    Reboots the instance only when the parameter is not yet in-sync.
    Uses test_decoding plugin (compatible with RDS without manual slot setup).
    """
    log("STEP 0.4 — Enabling logical replication on RDS (required for DMS)")
    pg_name = "pg16-logical-replication"
    db_id   = config["rds"]["identifier"]

    try:
        rds.describe_db_parameter_groups(DBParameterGroupName=pg_name)
        log("  Parameter group already exists — skipping creation")
    except rds.exceptions.DBParameterGroupNotFoundFault:
        rds.create_db_parameter_group(
            DBParameterGroupName=pg_name,
            DBParameterGroupFamily="postgres16",
            Description="Enable logical replication for DMS"
        )
        log("  Created parameter group: " + pg_name)

    rds.modify_db_parameter_group(
        DBParameterGroupName=pg_name,
        Parameters=[{
            "ParameterName":  "rds.logical_replication",
            "ParameterValue": "1",
            "ApplyMethod":    "pending-reboot"
        }]
    )
    log("  Parameter rds.logical_replication set to 1")

    instance   = rds.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]
    pg_info    = instance["DBParameterGroups"][0]
    current_pg = pg_info["DBParameterGroupName"]
    pg_status  = pg_info["ParameterApplyStatus"]

    log(f"  Current parameter group: {current_pg} | Status: {pg_status}")

    if current_pg != pg_name:
        log("  Attaching parameter group to RDS instance and syncing password...")
        rds.modify_db_instance(
            DBInstanceIdentifier=db_id,
            DBParameterGroupName=pg_name,
            ApplyImmediately=True,
            MasterUserPassword=DYNAMIC_DB_PASSWORD  # <-- THIS FIXES THE ERROR
        )
        log("  Giving AWS 30 seconds to begin the modification process...")
        time.sleep(30)
        log("  Waiting for modification to complete before rebooting...")
        waiter = rds.get_waiter("db_instance_available")
        waiter.wait(DBInstanceIdentifier=db_id)
        pg_status = "pending-reboot"

    if pg_status != "in-sync":
        log("  Rebooting RDS to apply logical replication parameter...")
        rds.reboot_db_instance(DBInstanceIdentifier=db_id)

        log("  Waiting for RDS to come back online...")
        waiter = rds.get_waiter("db_instance_available")
        waiter.wait(DBInstanceIdentifier=db_id)

        instance  = rds.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]
        pg_status = instance["DBParameterGroups"][0]["ParameterApplyStatus"]
        log(f"  Post-reboot parameter status: {pg_status}")

        if pg_status != "in-sync":
            raise RuntimeError(
                f"Logical replication parameter failed to apply. "
                f"Status: {pg_status}. Check RDS parameter group in AWS console."
            )
    else:
        log("  Parameter already in-sync — skipping reboot")

    log("  Logical replication is active and in-sync on RDS")


# ─────────────────────────────────────────────
# STEP 0.5 — Provision RDS instance
# ─────────────────────────────────────────────

def create_rds_database():
    log("STEP 0.5 — Setting up RDS PostgreSQL Database")
    rds_cfg = config["rds"]
    net_cfg = config["network"]

    global DYNAMIC_DB_ENDPOINT

    try:
        rds.describe_db_instances(DBInstanceIdentifier=rds_cfg["identifier"])
        log(f"  Database '{rds_cfg['identifier']}' already exists.")
    except rds.exceptions.DBInstanceNotFoundFault:
        log(f"  Restoring RDS instance from snapshot...")
        # We use restore_db_instance_from_db_snapshot instead of create_db_instance
        rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier = rds_cfg["identifier"],
            DBSnapshotIdentifier = "postgres-to-s3-db-snapshot", # The exact name from your screenshot
            DBInstanceClass      = rds_cfg["instance_class"],
            VpcSecurityGroupIds  = [net_cfg["security_group_id"]],
            PubliclyAccessible   = True
        )

    log("  Waiting for database to become available (5-10 mins)...")
    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(DBInstanceIdentifier=rds_cfg["identifier"])

    instances = rds.describe_db_instances(DBInstanceIdentifier=rds_cfg["identifier"])
    DYNAMIC_DB_ENDPOINT = instances["DBInstances"][0]["Endpoint"]["Address"]
    log(f"  Database is ready at: {DYNAMIC_DB_ENDPOINT}")
