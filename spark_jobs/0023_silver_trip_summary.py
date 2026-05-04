from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, min as _min, max as _max, when, coalesce, lit
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
    "detail": "trip_summary"
}))

spark = SparkSession.builder.appName("Silver STrip Summary Job") \
    .config("spark.sql.catalog.tsrtc", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.tsrtc.type", "hadoop") \
    .config("spark.sql.catalog.tsrtc.warehouse", "gs://de-iceberg-lakehouse/tsrtc") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.conf.set("spark.sql.iceberg.write.fanout.enabled", "true")

start_time = datetime.utcnow()

ticket_stage = spark.read.table("tsrtc.silver.ticket_stage").alias("ts")
service = spark.read.table("tsrtc.bronze.dim_service").alias("s")

ingest_time = datetime.utcnow()
ingest_date = ingest_time.date()

logger.info(json.dumps({
    "event": "transformation_started"
    }))

# Aggregate per trip
trip_summary = ticket_stage.groupBy(
    "trip_key",
    "service_key",
    "date_key",
    "depot_id"
).agg(
    _sum("passenger_count").alias("total_passengers"),
    _sum("revenue").alias("total_revenue"),
    coalesce(_sum("distance_travelled"), lit(0)).alias("total_km"),
    _min("from_stage_seq_no").alias("first_stage_seq"),
    _max("to_stage_seq_no").alias("last_stage_seq")
)

# Join service table to get seating capacity
trip_summary = trip_summary.join(
    service.select(col("service_key"),col("capacity").alias("seating_capacity")),
    "service_key",
    "left"
)

# EPKM
trip_summary = trip_summary.withColumn(
    "epkm",
    when(col("total_km") != 0, col("total_revenue")/col("total_km")).otherwise(0)
)

# Occupancy
trip_summary = trip_summary.withColumn(
    "occupancy_ratio",
    when(coalesce(col("seating_capacity"), lit(0)) != 0, col("total_passengers") / col("seating_capacity")).otherwise(0)
)

# Final Columns
trip_summary = trip_summary.select(
    "trip_key",
    "service_key",
    "date_key",
    "depot_id",
    "total_passengers",
    "total_revenue",
    "total_km",
    "first_stage_seq",
    "last_stage_seq",
    "epkm",
    "occupancy_ratio"
)

trip_summary = trip_summary.withColumn("silver_load_ts", lit(ingest_time))
trip_summary = trip_summary.withColumn("silver_load_date", lit(ingest_date))

trip_summary = trip_summary.repartition("date_key") \
    .sortWithinPartitions("date_key", "service_key", "trip_key")
    
logger.info(json.dumps({
    "event": "transformation_complete",
    "detail": "trip_summary"
    }))

table_name = "tsrtc.silver.trip_summary"

if not spark.catalog.tableExists(table_name):
    trip_summary.writeTo(table_name).partitionedBy("date_key").tableProperty("format-version","2").create()
else:
    trip_summary.writeTo(table_name).overwritePartitions()
    
logger.info(json.dumps({
    "event": "silver_job_complete",
    "detail": "trip_summary",
    "table": table_name,
    "duration_sec": (datetime.utcnow() - start_time).total_seconds()
}))

spark.stop()