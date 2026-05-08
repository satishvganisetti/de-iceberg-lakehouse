from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, lit
from datetime import datetime

import logging
import sys
import json

def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(handler)
        
    return logger

logger = get_logger(__name__)

logger.info(json.dumps({
    "event": "gold_job_start",
    "detail": "depot_day"
}))

spark = SparkSession.builder.appName("Gold Stage Day Job") \
    .config("spark.sql.catalog.tsrtc", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.tsrtc.type", "hadoop") \
    .config("spark.sql.catalog.tsrtc.warehouse", "gs://de-iceberg-lakehouse/tsrtc") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.conf.set("spark.sql.iceberg.write.fanout.enabled", "true")

start_time = datetime.utcnow()

ingest_time = datetime.utcnow()
ingest_date = ingest_time.date()

logger.info(json.dumps({
    "event": "reading_silver_stage_traffic"
    }))

stage_traffic = spark.read.table("tsrtc.silver.stage_traffic")

logger.info(json.dumps({
    "event": "transformation_started",
    "detail": "stage_day"
    }))

stage_day = stage_traffic.groupBy(
    "stage_key",
    "date_key"
).agg(
    countDistinct("trip_key").alias("trips"),
    _sum("boardings").alias("boardings"),
    _sum("alightings").alias("alightings"),
    _sum("revenue").alias("revenue")
)

stage_day = stage_day.withColumn(
    "net_flow",
    col("boardings") - col("alightings")
)

stage_day = stage_day.repartition("date_key") \
                     .sortWithinPartitions("date_key", "stage_key")

stage_day = stage_day.withColumn("gold_load_ts", lit(ingest_time))
stage_day = stage_day.withColumn("gold_load_date", lit(ingest_date))

logger.info(json.dumps({
    "event": "transformation_complete",
    "detail": "stage_day"
    }))   

table_name = "tsrtc.gold.stage_day"

if not spark.catalog.tableExists(table_name):
    (
        stage_day.writeTo(table_name)
        .partitionedBy("date_key")
        .tableProperty("format-version", "2")
        .create()
    )
else:
    (
        stage_day.writeTo(table_name)
        .overwritePartitions()
    )
    
logger.info(json.dumps({
    "event": "gold_job_complete",
    "detail": "stage_day",
    "table": table_name,
    "duration_sec": (datetime.utcnow() - start_time).total_seconds()
}))
    
spark.stop()