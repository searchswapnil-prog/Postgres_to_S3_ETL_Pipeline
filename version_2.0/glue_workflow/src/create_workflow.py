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

    print(f"Creating Workflow: {workflow_name}")
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
