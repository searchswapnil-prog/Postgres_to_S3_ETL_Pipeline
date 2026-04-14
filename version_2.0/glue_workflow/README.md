# Glue Workflow IAC

This module uses Infrastructure as Code (IAC) to manage the AWS Glue workflow instead of relying on the AWS Visual Interface.

## Structure
- `config/`: Contains YAML definitions for the workflow.
- `src/`: Python scripts to orchestrate boto3 commands.

## Usage
1. Modify the YAML files to adjust triggers or add jobs.
2. Run `python src/create_workflow.py` to compile the resources in your AWS account.
3. Run `python src/run_workflow.py` to trigger an execution.
