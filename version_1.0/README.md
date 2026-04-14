# Postgres → S3 → Iceberg Pipeline

## Project Structure

```
postgres_s3_pipeline/
├── config/
│   ├── infra_config.json     # AWS infra config (RDS, DMS, S3, IAM, network)
│   └── glue_config.json      # Glue jobs config (tables, PII, DQ, Iceberg)
│
├── infra/
│   ├── setup_rds.py          # RDS + Secrets Manager + Security Group rules
│   ├── setup_iam.py          # IAM role + trust + inline policies
│   ├── setup_s3.py           # S3 bucket + folder scaffold + script upload
│   └── setup_dms.py          # DMS replication instance, endpoints, task
│
├── glue_jobs/                # PySpark scripts — uploaded to s3://.../scripts/
│   ├── job_1_profiling.py
│   ├── job_2_deduplication.py
│   ├── job_3_pii_masking.py
│   ├── job_4_etl.py
│   ├── job_5_dq_check.py
│   └── job_6_iceberg_load.py
│
├── setup_pipeline.py         # Run once: provision all AWS infra
└── run_pipeline.py           # Run to execute the Glue pipeline
```

## Usage

### 1. First-time setup (provision AWS infrastructure)
```bash
python setup_pipeline.py
```

### 2. Run the Glue pipeline
```bash
python run_pipeline.py
```

### Uploading updated Glue scripts to S3
If you edit a script in `glue_jobs/`, re-upload with:
```bash
python -c "import sys; sys.path.insert(0,'infra'); import setup_s3; setup_s3.upload_scripts()"
```
