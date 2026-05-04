from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, lit, coalesce
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
    "event": "silver_job_start",
    "detail": "stage_traffic"
}))

spark = SparkSession.builder.appName("Silver Stage Traffic Job") \
    .config("spark.sql.catalog.tsrtc", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.tsrtc.type", "hadoop") \
    .config("spark.sql.catalog.tsrtc.warehouse", "gs://de-iceberg-lakehouse/tsrtc") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.conf.set("spark.sql.iceberg.write.fanout.enabled", "true")

start_time = datetime.utcnow()

logger.info(json.dumps({
    "event": "reading_silver_table",
    "detail": "ticket_stage"
}))

ticket_stage = spark.read.table("tsrtc.silver.ticket_stage")

ingest_time = datetime.utcnow()
ingest_date = ingest_time.date()

logger.info(json.dumps({
    "event": "transformation_started"
    }))

logger.info(json.dumps({
    "event": "silver_table_transformation",
    "detail": "ticket_stage_boardings"
}))

# Boarding
boardings = ticket_stage.select(
    col("trip_key"),
    col("service_key"),
    col("date_key"),
    col("depot_id"),
    coalesce(col("from_stop_key"), lit(0)).alias("stage_key"),
    col("passenger_count").alias("boardings"),
    lit(0).alias("alightings"),
    col("revenue")
)

logger.info(json.dumps({
    "event": "silver_table_transformation",
    "detail": "ticket_stage_alightings"
}))

# Alightings
alightings = ticket_stage.select(
    col("trip_key"),
    col("service_key"),
    col("date_key"),
    col("depot_id"),
    coalesce(col("to_stop_key"), lit(0)).alias("stage_key"),
    lit(0).alias("boardings"),
    col("passenger_count").alias("alightings"),
    lit(0).alias("revenue")
)

# Union
stage_traffic = boardings.union(alightings)

# Aggregate per stage, per trip, per day
stage_traffic = stage_traffic.groupBy(
    "trip_key",
    "service_key",
    "date_key",
    "depot_id",
    "stage_key"
).agg(
    _sum("boardings").alias("boardings"),
    _sum("alightings").alias("alightings"),
    _sum("revenue").alias("revenue")
)

stage_traffic = stage_traffic.withColumn("silver_load_ts", lit(ingest_time))
stage_traffic = stage_traffic.withColumn("silver_load_date", lit(ingest_date))

logger.info(json.dumps({
    "event": "transformation_complete",
    "detail": "stage_traffic"
    }))

table_name = "tsrtc.silver.stage_traffic"

stage_traffic = stage_traffic.repartition(col("date_key")) \
    .sortWithinPartitions("date_key", "trip_key", "stage_key")

if not spark.catalog.tableExists(table_name):
    stage_traffic.writeTo(table_name).partitionedBy("date_key").tableProperty("format-version", "2").create()
else:
    stage_traffic.writeTo(table_name).overwritePartitions()
    
logger.info(json.dumps({
    "event": "silver_job_complete",
    "detail": "stage_traffic",
    "table": table_name,
    "duration_sec": (datetime.utcnow() - start_time).total_seconds()
}))

spark.stop()