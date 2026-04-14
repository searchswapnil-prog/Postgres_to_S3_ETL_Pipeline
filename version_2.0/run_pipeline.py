"""
run_pipeline.py
───────────────
Executes the Glue Workflow configured in version 2.0 using the run_workflow script.
"""
import sys
import os

def main():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "glue_workflow", "src"))
    import run_workflow
    run_workflow.run_workflow()

if __name__ == "__main__":
    main()
