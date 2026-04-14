"""
infra/setup_iam.py
──────────────────
Manages the shared IAM role used by both DMS and Glue:
  - Creates the role if it doesn't exist
  - Sets trust policy (DMS + Glue can both assume it)
  - Attaches inline policies: S3 access, Glue catalog, EC2 SG rules
  - Applies the Glue catalog Iceberg policy (idempotent, safe to re-run)
"""

import boto3
import json
import time

with open("config/infra_config.json") as f:
    config = json.load(f)

region     = config["aws"]["region"]
account_id = config["aws"]["account_id"]
BUCKET     = config["s3"]["bucket"]
ROLE_NAME  = config["iam"]["role_name"]

iam = boto3.client("iam")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


# ─────────────────────────────────────────────
# STEP 3 — IAM Role (DMS + Glue trust)
# ─────────────────────────────────────────────

def create_iam_role():
    log("STEP 3 — Setting up IAM role")

    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "dms.amazonaws.com"},  "Action": "sts:AssumeRole"},
            {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}
        ]
    }

    try:
        iam.get_role(RoleName=ROLE_NAME)
        iam.update_assume_role_policy(
            RoleName=ROLE_NAME,
            PolicyDocument=json.dumps(assume_role_policy)
        )
        log(f"  Role '{ROLE_NAME}' exists — trust policy updated")
    except iam.exceptions.NoSuchEntityException:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        log(f"  Role '{ROLE_NAME}' created")

    # S3, Glue, CloudWatch, EC2 SG permissions
    inline_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"]
            },
            {
                "Effect": "Allow",
                "Action": ["glue:*", "logs:*", "cloudwatch:*"],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": ["ec2:DescribeSecurityGroups", "ec2:AuthorizeSecurityGroupIngress"],
                "Resource": "*"
            }
        ]
    }

    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="dms-s3-inline-policy",
        PolicyDocument=json.dumps(inline_policy)
    )
    log("  Inline policy 'dms-s3-inline-policy' applied")

    _apply_glue_catalog_policy()
    log("  IAM role setup complete")


# ─────────────────────────────────────────────
# Glue Catalog Iceberg policy (also called by run_pipeline)
# ─────────────────────────────────────────────

def _apply_glue_catalog_policy():
    """
    Grants the role permission to read/write the Glue Data Catalog —
    required for Iceberg table operations. Idempotent.
    """
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
                    f"arn:aws:glue:{region}:{account_id}:catalog",
                    f"arn:aws:glue:{region}:{account_id}:database/glue_iceberg_db",
                    f"arn:aws:glue:{region}:{account_id}:table/glue_iceberg_db/*"
                ]
            }
        ]
    }

    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="glue-catalog-iceberg-policy",
        PolicyDocument=json.dumps(glue_catalog_policy)
    )
    log(f"  Inline policy 'glue-catalog-iceberg-policy' applied to: {ROLE_NAME}")
