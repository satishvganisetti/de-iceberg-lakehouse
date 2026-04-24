# DE Iceberg Lakehouse Project

## Overview
End-to-end data pipeline:
- Source: Oracle ATP (JDBC)
- Processing: Spark (Dataproc)
- Storage: Iceberg on GCS
- Orchestration: Airflow

## Pipeline
1. Create Dataproc cluster
2. Run Spark job (bronze ingestion)
3. Delete cluster

## Structure
- dags/ → Airflow DAGs
- spark_jobs/ → PySpark jobs
- utils/ → shared utilities (secrets, etc.)
- scripts/ → cluster init scripts

## Features
- Secrets handled via GCP Secret Manager
- Ephemeral cluster (cost optimized)
- Modular code structure

## Bronze Layer Pipeline
- Source: Oracle ATP DB
- Target: Iceberg tables on GCS
- Orchestration: Airflow (Dataproc jobs)
- Supports:
  - Full refresh for dimensions
  - Partition overwrite for fact tables
- Logging:
  - Structured JSON logs
  - Partition-level logging

## Dependencies

This pipeline depends on an external Python package:

- `utils.zip`
  - Must be uploaded to GCS
  - Should contain `utils/secrets.py`
  - Used for securely retrieving database credentials
  - Credentials are managed externally and passed at runtime via `--py-files`