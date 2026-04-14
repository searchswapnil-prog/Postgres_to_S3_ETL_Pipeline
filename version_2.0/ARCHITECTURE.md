# Pipeline Architecture v2.0

As requested, below is the architecture diagram for the fully declarative AWS Glue Workflow and pipeline infrastructure in `version_2.0`.

> [!TIP]
> **To generate the visual architecture in Draw.io:**
> 1. Open Draw.io
> 2. Click `Arrange > Insert > Advanced > Mermaid`
> 3. Paste the code block below inside the prompt!

```mermaid
graph TD
    %% Define Styles
    classDef aws fill:#FF9900,stroke:#232F3E,stroke-width:2px,color:white;
    classDef db fill:#336791,stroke:#232F3E,stroke-width:2px,color:white;
    classDef s3 fill:#569A31,stroke:#232F3E,stroke-width:2px,color:white;
    classDef data fill:#eee,stroke:#999,stroke-width:1px;

    %% Source Database
    RDS[("RDS PostgreSQL Database<br>Source System")]:::db
    
    %% DMS Component
    subgraph DMS["AWS DMS Replication"]
        DMS_Inst["DMS Replication Instance"]:::aws
        DMS_Task["Full Load & CDC Task"]:::aws
    end
    
    RDS -->|WAL/Logical Replication| DMS_Inst
    DMS_Inst --> DMS_Task
    
    %% S3 Data Lake Layers
    subgraph S3_Lake["S3 Data Lake Storage"]
        Raw["Raw Zone<br>(Parquet)"]:::s3
        Staging["Staging Zone<br>(Dedup + Masked)"]:::s3
        Processed["Processed Zone<br>(Joined Data)"]:::s3
        Iceberg["Iceberg Zone<br>(Apache Iceberg)"]:::s3
        Error["Error/Quarantine Zone<br>Failed DQ"]:::s3
    end
    
    DMS_Task -->|Write| Raw
    
    %% AWS Glue Workflow Orchestration
    subgraph Glue_Workflow["AWS Glue Workflow (v2.0)"]
        direction LR
        Job1["1. Profiling Job"]:::aws
        Job2["2. Deduplication Job"]:::aws
        Job3["3. PII Masking Job"]:::aws
        Job4["4. ETL Job"]:::aws
        Job5["5. Data Quality (DQ) Job"]:::aws
        Job6["6. Iceberg Load Job"]:::aws
        
        Job1 -->|Trigger| Job2
        Job2 -->|Trigger (dedup_ts)| Job3
        Job3 -->|Trigger (masked_ts)| Job4
        Job4 -->|Trigger (etl_ts)| Job5
        Job5 -->|Trigger (dq_status)| Job6
    end
    
    %% Glue Job Actions to S3
    Raw -.->|Read| Job1
    Raw -.->|Read| Job2
    Job2 -.->|Write| Staging
    Staging -.->|Read| Job3
    Job3 -.->|Write| Staging
    Staging -.->|Read| Job4
    Job4 -.->|Write| Processed
    Processed -.->|Read| Job5
    Job5 -.->|Write| Iceberg
    Job5 -.->|Route Fails| Error
    Iceberg -.->|Read| Job6
    Job6 -.->|Register Table Layer| Iceberg
```
