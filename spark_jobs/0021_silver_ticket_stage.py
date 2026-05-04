from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit, coalesce
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
    "detail": "ticket_stage"
}))

spark = SparkSession.builder.appName("Silver Ticket Stage Job") \
    .config("spark.sql.catalog.tsrtc", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.tsrtc.type", "hadoop") \
    .config("spark.sql.catalog.tsrtc.warehouse", "gs://de-iceberg-lakehouse/tsrtc") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()
   
logger.info(json.dumps({
    "event": "reading_bronze_tables"
}))

start_time = datetime.utcnow()

drs_from = spark.read.table("tsrtc.bronze.dim_route_stage").alias("drs_from")
drs_to = spark.read.table("tsrtc.bronze.dim_route_stage").alias("drs_to")
date = spark.read.table("tsrtc.bronze.dim_date").alias("d")
trip = spark.read.table("tsrtc.bronze.dim_trip").alias("t")
fct = spark.read.table("tsrtc.bronze.fct_ticket").alias("f")
    
ingest_time = datetime.utcnow()
ingest_date = ingest_time.date()

logger.info(json.dumps({
    "event": "transformation_started"
    }))
    
# join From Stage
silver = fct.join(
    broadcast(drs_from),
    (col("f.service_key") == col("drs_from.service_key")) &
    (col("f.from_stop_key") == col("drs_from.stage_key")),
    "left"
)
    
# join To Stage
silver = silver.join(
    broadcast(drs_to),
    (col("f.service_key") == col("drs_to.service_key")) &
    (col("f.to_stop_key") == col("drs_to.stage_key")),
    "left"
)
    
#join Trip
silver = silver.join(
    trip,
    (col("f.depot_id") == col("t.depot_key")) &
    (col("f.trip_no") == col("t.trip_no")) &
    (col("f.waybillno") == col("t.waybillno")),
    "left"
)
    
#join Date
silver = silver.join(
    date,
    (col("f.date_key") == col("d.date_key")),
    "left"
)
    
# Distance travelled between stages
silver = silver.withColumn(
    "distance_travelled",
    coalesce(col("drs_to.distance_from_origin"), lit(0)) -
    coalesce(col("drs_from.distance_from_origin"), lit(0))
)
    
silver = silver.withColumn(
    "passenger_count",
    coalesce(col("f.no_of_adults"), lit(0)) +
    coalesce(col("f.no_of_childs"), lit(0))
)
    
# Select final columns
silver_ticket_stage = silver.select(
    col("t.trip_key").alias("trip_key"),
    col("f.date_key").alias("date_key"),
    col("f.service_key").alias("service_key"),
    col("f.depot_id").alias("depot_id"),
    col("f.from_stop_key").alias("from_stop_key"),
    col("f.to_stop_key").alias("to_stop_key"),
    col("drs_from.stage_seq_no").alias("from_stage_seq_no"),
    col("drs_to.stage_seq_no").alias("to_stage_seq_no"),
    col("distance_travelled"),
    col("f.no_of_adults").alias("no_of_adults"),
    col("f.no_of_childs").alias("no_of_childs"),
    col("passenger_count"),
    col("f.revenue").alias("revenue"),
    col("f.trip_kms").alias("trip_kms")
)

silver_ticket_stage = silver_ticket_stage \
    .withColumn("silver_load_ts", lit(ingest_time)) \
    .withColumn("silver_load_date", lit(ingest_date))

logger.info(json.dumps({
    "event": "transformation_complete"
}))

spark.sql("CREATE DATABASE IF NOT EXISTS tsrtc.silver")

table_name = "tsrtc.silver.ticket_stage"

if not spark.catalog.tableExists(table_name):
    silver_ticket_stage.writeTo(table_name).partitionedBy("date_key").create()
else:
    silver_ticket_stage.writeTo(table_name).overwritePartitions()


logger.info(json.dumps({
    "event": "partition_written",
    "table": table_name
}))

logger.info(json.dumps({
    "event": "silver_job_complete",
    "detail": "ticket_stage",
    "table": table_name,
    "duration_sec": (datetime.utcnow() - start_time).total_seconds()
}))

spark.stop()