import boto3
import yaml
import time
import os
import json

def create_workflow():
    region = "ap-south-1"
    try:
        infra_path = os.path.join(os.path.dirname(__file__), "../../config/infra_config.json")
        with open(infra_path) as f:
            infra = json.load(f)
            region = infra["aws"]["region"]
    except Exception:
        pass

    glue = boto3.client("glue", region_name=region)
    config_path = os.path.join(os.path.dirname(__file__), "../config/glue_create_workflow_config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    workflow_name = config["workflow_name"]

    bucket = infra["s3"]["bucket"]
    account_id = infra["aws"]["account_id"]
    iam_role = infra["iam"]["role_name"]
    iam_arn = f"arn:aws:iam::{account_id}:role/{iam_role}"
    config_s3 = f"s3://{bucket}/config/glue_config.json"
    
    iceberg_conf = (
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        " --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog"
        " --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
        f" --conf spark.sql.catalog.glue_catalog.warehouse=s3://{bucket}/iceberg"
    )

    common_args = {
        "--conf": iceberg_conf,
        "--datalake-formats": "iceberg",
        "--enable-glue-datacatalog": "true",
        "--config_s3_path": config_s3
    }

    print("Creating/Updating underlying Glue Jobs...")
    for job_name in config.get("jobs", []):
        job_config = {
            "Role": iam_arn,
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": f"s3://{bucket}/scripts/{job_name}.py",
                "PythonVersion": "3"
            },
            "GlueVersion": "4.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "DefaultArguments": common_args
        }
        try:
            glue.get_job(JobName=job_name)
            glue.update_job(JobName=job_name, JobUpdate=job_config)
            print(f"Updated job: {job_name}")
        except glue.exceptions.EntityNotFoundException:
            glue.create_job(Name=job_name, **job_config)
            print(f"Created job: {job_name}")

    print(f"\nCreating Workflow: {workflow_name}")
    try:
        glue.get_workflow(Name=workflow_name)
        glue.delete_workflow(Name=workflow_name)
        time.sleep(3)
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_workflow(
        Name=workflow_name,
        Description=config.get("description", "Automated Workflow")
    )
    print(f"Workflow {workflow_name} created.")

    for trigger in config.get("triggers", []):
        trigger_name = f"{workflow_name}-{trigger['name']}"
        trigger_type = trigger["type"]
        actions = [{"JobName": a} for a in trigger["actions"]]
        
        kwargs = {
            "Name": trigger_name,
            "Type": trigger_type,
            "WorkflowName": workflow_name,
            "Actions": actions,
        }

        if trigger_type == "CONDITIONAL":
            conditions = [{"LogicalOperator": "EQUALS", "JobName": cond["job_name"], "State": cond["state"]} for cond in trigger["conditions"]]
            kwargs["Predicate"] = {"Logical": "ANY", "Conditions": conditions}
            kwargs["StartOnCreation"] = True
            
        glue.create_trigger(**kwargs)
        print(f"Created Trigger: {trigger_name}")

    print("Workflow IAC setup complete.")

if __name__ == "__main__":
    create_workflow()
