from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_ID = "project-d8285645-02d2-4be2-a85"
REGION = "asia-south1"
CLUSTER_NAME = "iceberg-cluster"

with DAG(
        dag_id = "pipeline_trial",
        start_date=datetime(2026,1,1),
        schedule_interval=None,
        catchup=False
) as dag:

        create_spark_cluster=BashOperator(
                task_id="create_cluster",
                bash_command=f"""
                gcloud dataproc clusters create {CLUSTER_NAME} \
                --region={REGION} \
                --single-node \
                --master-machine-type=e2-standard-2 \
                --image-version=2.1-debian11 \
                --bucket=de-iceberg-lakehouse \
                --initialization-actions=gs://de-iceberg-lakehouse/scripts/init-iceberg.sh \
                --enable-component-gateway
                """
        )

        bronze_small = BashOperator(
                task_id="bronze_small",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/01_bronze_load_sqlserver_small.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )

        delete_cluster=BashOperator(
                task_id="delete_cluster",
                bash_command=f"""
                gcloud  dataproc clusters delete {CLUSTER_NAME} \
                --region={REGION} \
                --quiet
                """,
                trigger_rule="all_done"
        )

        create_spark_cluster >> bronze_small >> delete_cluster