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