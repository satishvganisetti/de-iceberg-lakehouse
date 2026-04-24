from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

from utils.secrets import get_secret

import logging
import sys
import json
import time

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

spark = SparkSession.builder.appName("Oracle DB to Bronze layer") \
    .config("spark.sql.catalog.tsrtc", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.tsrtc.type", "hadoop") \
    .config("spark.sql.catalog.tsrtc.warehouse", "gs://de-iceberg-lakehouse/tsrtc") \
    .getOrCreate()
    
jdbc_url = """jdbc:oracle:thin:@(description=
(retry_count=20)
(retry_delay=3)
(address=(protocol=tcps)(port=1522)(host=adb.ap-mumbai-1.oraclecloud.com))
(connect_data=(service_name=g602cdd981ea6ff_atpdb_medium.adb.oraclecloud.com))
(security=(ssl_server_dn_match=yes))
)"""

password = get_secret("oracle-db-password")

connection_properties = {
    "user": "TSRTC_ROUTE",
    "password": password,
    "driver": "oracle.jdbc.driver.OracleDriver"
}

dim_tables = [
    'DIM_DATE', 
    'DIM_DEPOT', 
    'DIM_ROUTE_STAGE', 
    'DIM_SERVICE', 
    'DIM_SERVICE_PLAN', 
    'DIM_STAGE', 
    'DIM_TIME', 
    'DIM_TRIP'
    ]

fact_tables = ["FCT_TICKET"]

spark.sql("CREATE NAMESPACE IF NOT EXISTS tsrtc.bronze")

ingest_time = datetime.utcnow()
ingest_date = ingest_time.date()

logger.info("Starting bronze ingestion job")

for tab in dim_tables:
    start_time = time.time()
    logger.info(f"Reading table: {tab}")
    
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"TSRTC_ROUTE.{tab}",
            numPartitions=1,
            properties=connection_properties
        )
    
        logger.info(json.dumps({
            "event":"table_read_started",
            "table":tab
            }))
    
        logger.info("Adding ingestion time and source")
        
        df = df \
            .withColumn("ingestion_time", lit(ingest_time)) \
            .withColumn("load_date", lit(ingest_date)) \
            .withColumn("source_db", lit("Oracle ATP DB"))
        
        df.writeTo(f"tsrtc.bronze.{tab.lower()}").createOrReplace()
        logger.info(json.dumps({
            "event":"table_write_success",
            "table":tab,
            "duration_sec":round(time.time() - start_time,2)
            }))
        
    except Exception as e:
        logger.exception(json.dumps({
            "event":"table_failed",
            "table":tab
            }))
        
for tab in fact_tables:
    start_time = time.time()
    logger.info(f"Reading table: {tab}")
    
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"TSRTC_ROUTE.{tab}",
            column="DATE_KEY",
            lowerBound=20230401,
            upperBound=20230731,
            numPartitions=4,
            properties=connection_properties
            )
        
        logger.info(json.dumps({
            "event":"table_read_started",
            "table":tab
            }))
        
        logger.info("Adding ingestion time and source")
        
        df = df \
            .withColumn("ingestion_time", lit(ingest_time)) \
            .withColumn("load_date", lit(ingest_date)) \
            .withColumn("source_db", lit("Oracle ATP DB"))
            
        dates = [row["DATE_KEY"] for row in df.select("DATE_KEY").distinct().collect()]
            
        if not spark.catalog.tableExists(f"tsrtc.bronze.{tab.lower()}"):
            df.writeTo(f"tsrtc.bronze.{tab.lower()}").partitionedBy("DATE_KEY").create()
            
        else:
            df.writeTo(f"tsrtc.bronze.{tab.lower()}").overwritePartitions()
        
        logger.info(json.dumps({
            "event":"table_write_success",
            "table":tab,
            "duration_sec":round(time.time() - start_time,2),
            "message":"Partitions Overwritten"
            }))
          
        logger.info(json.dumps({
            "event": "partitions_written",
            "table": tab,
            "partitions": dates,
            "num_partitions": len(dates)
        }))
        
    except Exception as e:
        logger.exception(json.dumps({
            "event":"table_failed",
            "table":tab
            }))
        
spark.stop()