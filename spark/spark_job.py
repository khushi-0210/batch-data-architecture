from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToHDFSProcessor") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema of the Kafka message (adjust according to your CSV format)
schema = StructType() \
    .add("pickup_datetime", StringType()) \
    .add("dropoff_datetime", StringType()) \
    .add("passenger_count", StringType()) \
    .add("trip_distance", StringType()) \
    .add("payment_type", StringType()) \
    .add("fare_amount", StringType())

# 3. Read stream from Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nyc_trip_data") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Decode and parse value
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Write output to HDFS
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/user/data/output") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
