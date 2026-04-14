import boto3
import yaml
import os
import json

def run_workflow():
    region = "ap-south-1"
    try:
        infra_path = os.path.join(os.path.dirname(__file__), "../../config/infra_config.json")
        with open(infra_path) as f:
            infra = json.load(f)
            region = infra["aws"]["region"]
    except Exception:
        pass

    glue = boto3.client("glue", region_name=region)
    config_path = os.path.join(os.path.dirname(__file__), "../config/glue_run_workflow_config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    workflow_name = config["workflow_name"]

    print(f"Starting workflow: {workflow_name}")
    response = glue.start_workflow_run(Name=workflow_name)
    print(f"Workflow Run ID: {response['RunId']}")

if __name__ == "__main__":
    run_workflow()
