import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- 1. WINDOWS HADOOP FIX (CRITICAL) ---
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['PATH'] += os.pathsep + "C:\\hadoop\\bin"
checkpoint_path = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark_checkpoints")
# --- 2. INFLUXDB CONFIGURATION ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "dDKt36p-oYnheiM38fqTR5dlTzht9j7r4jg6yG9hhodSZrnRIrlZUO9Ie3sHHEBKq70ggbNvza0mFqT39IDLkw=="  # Replace with your generated token
INFLUX_ORG = "space_org"
INFLUX_BUCKET = "solar_data"

def write_to_influx(batch_df, batch_id):
    """Function called by Spark for every micro-batch."""
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Convert Spark rows to InfluxDB Points
    for row in batch_df.collect():
        point = Point("solar_flux") \
            .tag("flare_class", row.flare_class) \
            .field("flux_value", float(row.flux)) \
            .time(row.time_tag)
        
        write_api.write(bucket=INFLUX_BUCKET, record=point)
    
    client.close()
    print(f"✅ Batch {batch_id} sent to InfluxDB")

# --- 3. SPARK SESSION SETUP (SPARK 4.0) ---
spark = SparkSession.builder \
    .appName("SolarFlareProcessor") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 4. DATA INGESTION (KAFKA/REDPANDA) ---
schema = StructType([
    StructField("time_tag", StringType()),
    StructField("flux", DoubleType()),
    StructField("energy", StringType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:19092") \
    .option("subscribe", "solar-flux") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- 5. TRANSFORMATION & CLASSIFICATION ---
processed_df = raw_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("flare_class", 
        when(col("flux") >= 1e-4, "X-Class")
        .when(col("flux") >= 1e-5, "M-Class")
        .when(col("flux") >= 1e-6, "C-Class")
        .otherwise("Normal")
    )

# --- 6. START THE STREAMING SINK ---
# Use a specific local path to avoid permission errors
#checkpoint_path = "C:\\spark_checkpoints"
#if not os.path.exists(checkpoint_path):
#    os.makedirs(checkpoint_path)

query = processed_df.writeStream \
    .foreachBatch(write_to_influx) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='10 seconds') \
    .start()

print("🛰️  Mission Control Active: Processing Solar Flux Data...")
query.awaitTermination()