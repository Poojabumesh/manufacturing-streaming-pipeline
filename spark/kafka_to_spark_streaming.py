from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("KafkaIoTStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("flow_rate", DoubleType(), True)
])

# 3. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot_telemetry") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert value from bytes to string
json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# 5. Parse JSON and apply schema
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert event_time (string) to a real timestamp column
with_ts = parsed_df.withColumn("event_time_ts", to_timestamp(col("event_time")))

# Watermark + deduplication (idempotency)
deduped = with_ts \
    .withWatermark("event_time_ts", "10 minutes") \
    .dropDuplicates(["batch_id", "equipment_id", "event_time_ts"])


def log_batch(df, batch_id):
    print(f"\n==== Spark micro-batch: {batch_id} | rows: {df.count()} ====\n")



# 6. Write to console (for now)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("Console") \
    .option("truncate", False) \
    .option("checkpointLocation", "spark/checkpoints/iot_telemetry_console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
