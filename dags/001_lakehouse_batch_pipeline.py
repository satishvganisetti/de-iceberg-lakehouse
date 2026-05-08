from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_ID = "project-d8285645-02d2-4be2-a85"
REGION = "asia-south1"
CLUSTER_NAME = "iceberg-cluster"

default_args = {
    "retries": 1
}

with DAG(
        dag_id = "lakehouse_batch_pipeline",
        start_date=datetime(2026,1,1),
        schedule_interval=None,
        catchup=False,
        default_args=default_args
) as dag:

        create_spark_cluster=BashOperator(
                task_id="create_cluster",
                bash_command=f"""
                gcloud dataproc clusters create {CLUSTER_NAME} \
                --region={REGION} \
                --single-node \
                --master-machine-type=e2-standard-4 \
                --image-version=2.1-debian11 \
                --bucket=de-iceberg-lakehouse \
                --initialization-actions=gs://de-iceberg-lakehouse/scripts/init-iceberg.sh \
                --enable-component-gateway
                """
        )

        bronze_ingestion = BashOperator(
                task_id="bronze_ingestion",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/001_oracle_to_bronze_layer.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )
        
        silver_ticket_stage = BashOperator(
                task_id="silver_ticket_stage",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0021_silver_ticket_stage.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )
        
        silver_stage_traffic = BashOperator(
                task_id="silver_stage_traffic",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0022_silver_stage_traffic.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )
        
        silver_trip_summary = BashOperator(
                task_id="silver_trip_summary",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0023_silver_trip_summary.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )

        gold_route_day = BashOperator(
                task_id="gold_route_day",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0031_gold_route_day.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )
        
        gold_depot_day = BashOperator(
                task_id="gold_depot_day",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0032_gold_depot_day.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )
        
        gold_stage_day = BashOperator(
                task_id="gold_stage_day",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0033_gold_stage_day.py \
                --cluster={CLUSTER_NAME} \
                --region={REGION} \
                --py-files=gs://de-iceberg-lakehouse/utils.zip
                """
        )
        
        gold_kpi_daily = BashOperator(
                task_id="gold_kpi_daily",
                bash_command=f"""
                gcloud dataproc jobs submit pyspark \
                gs://de-iceberg-lakehouse/spark_jobs/0034_gold_kpi_daily.py \
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

        create_spark_cluster >> bronze_ingestion >> silver_ticket_stage >> silver_stage_traffic >> silver_trip_summary >> gold_route_day >> gold_depot_day >> gold_stage_day >> gold_kpi_daily >> delete_cluster