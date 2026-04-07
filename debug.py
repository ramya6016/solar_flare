from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys

# 1. Point to the folder CONTAINING the bin folder
os.environ['HADOOP_HOME'] = "C:\\hadoop"
# 2. Add the bin folder itself to the PATH so Windows finds hadoop.dll
os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"
# 1. Setup Spark for 4.0 with Localhost bindings
spark = SparkSession.builder \
    .appName("DebugConsumer") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

# 2. Read RAW data from Kafka (No Schema yet)
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "solar-flux") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Just cast the binary 'value' to a String
string_df = raw_df.selectExpr("CAST(value AS STRING)")

# 4. Write to Console with a 5-second trigger
query = string_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime='5 seconds') \
    .start()

print("🛰️  Listening for Redpanda data... (Batch 0 should appear soon)")
query.awaitTermination()