#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("EnviroHealth Stream Processor") \
    .master("yarn") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schemas
air_quality_schema = StructType([
    StructField("city", StringType(), True),
    StructField("location", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", StringType(), True)
])

weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("windspeed", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka - Air Quality
air_quality_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "openaq_air_quality") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
air_quality_parsed = air_quality_df \
    .select(from_json(col("value").cast("string"), air_quality_schema).alias("data")) \
    .select("data.*")

# Read from Kafka - Weather
weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_forecast") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
weather_parsed = weather_df \
    .select(from_json(col("value").cast("string"), weather_schema).alias("data")) \
    .select("data.*")

# Write to HDFS - Air Quality
air_quality_query = air_quality_parsed \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs:///envirohealth/raw/openaq") \
    .option("checkpointLocation", "hdfs:///envirohealth/checkpoints/openaq") \
    .outputMode("append") \
    .start()

# Write to HDFS - Weather
weather_query = weather_parsed \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs:///envirohealth/raw/weather") \
    .option("checkpointLocation", "hdfs:///envirohealth/checkpoints/weather") \
    .outputMode("append") \
    .start()

print("Streaming jobs started. Press Ctrl+C to stop...")
print("Air Quality: writing to hdfs:///envirohealth/raw/openaq")
print("Weather: writing to hdfs:///envirohealth/raw/weather")

# Wait for termination
spark.streams.awaitAnyTermination()
