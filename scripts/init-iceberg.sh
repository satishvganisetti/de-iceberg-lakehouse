#!/bin/bash
set -e

# Iceberg jar
gsutil cp gs://de-iceberg-lakehouse/jars/iceberg-spark-runtime-3.3_2.12-1.4.2.jar /usr/lib/spark/jars/

# Oracle JDBC driver
gsutil cp gs://de-iceberg-lakehouse/jars/ojdbc8.jar /usr/lib/spark/jars/

pip install google-cloud-secret-manager