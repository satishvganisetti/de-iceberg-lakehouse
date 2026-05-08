from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, when, coalesce, lit
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

spark = SparkSession.builder.appName("Gold Depot Day Job") \
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
    "event": "reading_silver_trip_summary"
    }))

trip_summary = spark.read.table("tsrtc.silver.trip_summary")

logger.info(json.dumps({
    "event": "transformation_started",
    "detail": "depot_day"
    }))

depot_day = trip_summary.groupBy(
    "depot_id",
    "date_key"
).agg(
    count("trip_key").alias("trips"),
    _sum("total_passengers").alias("passengers"),
    _sum("total_revenue").alias("revenue"),
    coalesce(_sum("total_km"), lit(0)).alias("total_km"),
    avg("occupancy_ratio").alias("avg_occupancy")
)

depot_day = depot_day.withColumn(
    "epkm",
    when(col("total_km") != 0, col("revenue") / col("total_km")).otherwise(0)
)

depot_day = depot_day.withColumn(
    "revenue_per_trip",
    when(col("trips") != 0, col("revenue") / col("trips")).otherwise(0)
)

depot_day = depot_day.withColumn(
    "passengers_per_trip",
    when(col("trips") != 0, col("passengers") / col("trips")).otherwise(0)
)

depot_day = depot_day.repartition("date_key") \
                     .sortWithinPartitions("date_key", "depot_id")

depot_day = depot_day.withColumn("gold_load_ts", lit(ingest_time))
depot_day = depot_day.withColumn("gold_load_date", lit(ingest_date))

logger.info(json.dumps({
    "event": "transformation_complete",
    "detail": "depot_day"
    }))   

table_name = "tsrtc.gold.depot_day"

if not spark.catalog.tableExists(table_name):
    (
        depot_day.writeTo(table_name)
        .partitionedBy("date_key")
        .tableProperty("format-version", "2")
        .create()
    )
else:
    (
        depot_day.writeTo(table_name)
        .overwritePartitions()
    )
    
logger.info(json.dumps({
    "event": "gold_job_complete",
    "detail": "depot_day",
    "table": table_name,
    "duration_sec": (datetime.utcnow() - start_time).total_seconds()
}))
    
spark.stop()