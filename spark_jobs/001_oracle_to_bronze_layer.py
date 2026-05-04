from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_date
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
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.conf.set("spark.sql.iceberg.write.distribution-mode", "none")
spark.conf.set("spark.sql.shuffle.partitions", "4")

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
        raise

# Reading distinct dates to address the memory issue. Large data is not processed in one attempt.  
start_time = time.time()
tab = "FCT_TICKET"

try:
    logger.info(f"Reading distinct DATE_KEYs for table: {tab}")

    date_df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT DISTINCT DATE_KEY FROM TSRTC_ROUTE.FCT_TICKET) tmp",
        properties=connection_properties
    )

    date_keys = sorted([int(row["DATE_KEY"]) for row in date_df.collect()])

    logger.info(json.dumps({
        "event": "date_keys_fetched",
        "table": tab,
        "count": len(date_keys)
    }))

except Exception as e:
    logger.exception(json.dumps({
        "event":"date_keys_failed",
        "table":tab
        }))
    raise

table_initialized = False

# Writing the rest date partitions 
for idx, dk in enumerate(date_keys, start=1):
    
    partition_start = time.time()
    logger.info(f"Processing DATE_KEY: {dk}")
             
    try:        
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"(SELECT * FROM TSRTC_ROUTE.FCT_TICKET WHERE DATE_KEY = {dk}) tmp",
            properties=connection_properties
        )
        
        df = df \
            .withColumn("DATE_KEY", col("DATE_KEY").cast("int")) \
            .withColumn(
                "event_date",
                to_date(col("DATE_KEY").cast("string"), "yyyyMMdd")
            ) \
            .withColumn("ingestion_time", lit(ingest_time)) \
            .withColumn("load_date", lit(ingest_date)) \
            .withColumn("source_db", lit("Oracle ATP DB"))
        
        if not table_initialized:
            try:
                df.writeTo("tsrtc.bronze.fct_ticket").partitionedBy("event_date").create()
                
                logger.info("Table created successfully")
                table_initialized = True
                
            except:
                if "already exists" in str(e):
                    logger.info("Table already exists, switching to overwrite")
                    df.writeTo("tsrtc.bronze.fct_ticket").overwritePartitions()
                    table_initialized = True
                else:
                    raise
        
        else:
            df.writeTo("tsrtc.bronze.fct_ticket").overwritePartitions()
        
        df.unpersist(blocking=True)
        spark.catalog.clearCache()

        logger.info(json.dumps({
            "event": "partition_write_success",
            "table": tab,
            "date_key": dk,
            "duration_sec": round(time.time() - partition_start, 2),
            "total_dates": len(date_keys),
            "processed_dates": idx
        }))
        
    except Exception as e:
        logger.exception(json.dumps({
            "event": "partition_failed",
            "table": tab,
            "date_key": dk
        }))
        raise

logger.info(json.dumps({
    "event": "table_completed",
    "table": tab,
    "total_duration_sec": round(time.time() - start_time, 2)
}))

spark.stop()