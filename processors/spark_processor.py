from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from influxdb import InfluxDBClient

# --- CONFIG ---
KAFKA_SERVER = "localhost:9092"
INFLUX_DB = "envirohealth"
HDFS_PATH = "hdfs://master:9000/datalake/processed_events"
CHECKPOINT_PATH = "/checkpoints/v1_stable"

# --- SCHEMAS ---
air_schema = StructType([
    StructField("city", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", StringType())
])

weather_schema = StructType([
    StructField("city", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("mode", StringType())
])

def write_to_influx(partition_iterator):
    client = InfluxDBClient(host="localhost", port=8086, database=INFLUX_DB)
    points = []
    for row in partition_iterator:
        r = row.asDict()
        pm25 = float(r.get('pm25', 0.0))
        temp = float(r.get('temperature', 20.0))
        # Logic: (70% Air + 30% Temp)
        hri = (pm25 * 0.7) + (temp * 0.3)
        
        points.append({
            "measurement": "health_metrics",
            "tags": {"city": r.get('city', 'Unknown')},
            "fields": {"pm25": pm25, "temp": temp, "hri": hri}
        })
    if points:
        client.write_points(points)
    client.close()

# --- MAIN ---
spark = SparkSession.builder \
    .appName("EnviroHealth-6Node-Stable") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.network.timeout", "1000s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Streams
df_air = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("subscribe", "openaq_air_quality").load() \
    .select(from_json(col("value").cast("string"), air_schema).alias("data")).select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

df_weather = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("subscribe", "weather_forecast").load() \
    .select(from_json(col("value").cast("string"), weather_schema).alias("data")).select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Resilient Left Join
df_final = df_air.alias("a").join(
    df_weather.alias("w"),
    expr("a.city = w.city AND a.timestamp >= w.timestamp - interval 10 minutes AND a.timestamp <= w.timestamp + interval 10 minutes"),
    "left"
).select(
    col("a.city"),
    col("a.value").alias("pm25"),
    when(col("w.temperature").isNull(), 20.0).otherwise(col("w.temperature")).alias("temperature"),
    when(col("w.windspeed").isNull(), 5.0).otherwise(col("w.windspeed")).alias("windspeed")
)

# Sinks
query_influx = df_final.writeStream.foreachBatch(lambda b, i: b.foreachPartition(write_to_influx)).trigger(processingTime='10 seconds').start()
query_hdfs = df_final.writeStream.format("json").option("path", HDFS_PATH).option("checkpointLocation", CHECKPOINT_PATH).trigger(processingTime='30 seconds').start()

spark.streams.awaitAnyTermination()