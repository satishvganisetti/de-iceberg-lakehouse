from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, lit

from utils.secrets import get_secret

spark = SparkSession.builder.appName("Spark Job Test") \
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

dim_tables = ["DIM_DATE", "DIM_DEPOT"]

spark.sql("CREATE NAMESPACE IF NOT EXISTS tsrtc.bronze")

ingest_date = current_date()
ingest_time = current_timestamp()

for tab in dim_tables:
    print(f"Reading table {tab}")
    
    df = spark.read.jdbc(
        url=jdbc_url,
        table=f"TSRTC_ROUTE.{tab}",
        numPartitions=1,
        properties=connection_properties
    )
    
    df = df \
        .withColumn("ingestion_date", ingest_date) \
        .withColumn("load_timestamp", ingest_time) \
        .withColumn("source_system", lit("Oracle ATP DB"))
    
    print(f"Writing {tab} to Iceberg...")
    
    df.writeTo(f"tsrtc.bronze.{tab.lower()}") \
      .createOrReplace()
    
    print(f"Finished {tab}")

spark.stop()