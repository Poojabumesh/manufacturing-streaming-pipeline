from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, avg, max as max_, count, window, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
import os

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
with_t = parsed_df.withColumn("event_time_ts", to_timestamp(col("event_time")))
with_ts = with_t.withColumn("INGESTED_AT", current_timestamp())


# Watermark + deduplication (idempotency)
deduped = with_ts \
    .withWatermark("event_time_ts", "10 minutes") \
    .dropDuplicates(["batch_id", "equipment_id", "event_time_ts"])

#aggregation
batch_agg = deduped \
	.groupBy(
		window(col("event_time_ts"), "15 minutes"), 
		col("batch_id")
	) \
	.agg(
		avg("temperature").alias("avg_temperature"), 
		max_("pressure").alias("max_pressure"), 
		avg("flow_rate").alias("avg_flow_rate"), 
		count("*").alias("event_count")
	)


def log_batch(df, batch_id):
    print(f"\n==== Spark micro-batch: {batch_id} | rows: {df.count()} ====\n")




#SNOWFLAKE:
sfOptions = {
    "sfURL": "fxc53199.us-east-1.snowflakecomputing.com",
    "sfUser": "FLAVORMETRICS",
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": "IOT_DB",
    "sfSchema": "RAW",
    "sfWarehouse": "IOT_WH",
    "sfRole": "ACCOUNTADMIN",
}

sfAggOptions = {
    **sfOptions,
    "sfSchema": "CURATED",
}

def write_raw_to_snowflake(df, batch_id):
    df = df.select(
        "event_time",
        "batch_id",
        "equipment_id",
        "temperature",
        "pressure",
        "flow_rate",
        "INGESTED_AT",
    )
    df.write \
      .format("net.snowflake.spark.snowflake") \
      .options(**sfOptions) \
      .option("dbtable", "IOT_EVENTS") \
      .mode("append") \
      .save()

def write_agg_to_snowflake(df, batch_id):
    df = df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("batch_id"),
        col("avg_temperature"),
        col("max_pressure"),
        col("avg_flow_rate"),
        col("event_count"),
        current_timestamp().alias("ingestion_time"),
    )
    df.write \
      .format("net.snowflake.spark.snowflake") \
      .options(**sfAggOptions) \
      .option("dbtable", "IOT_BATCH_METRICS") \
      .mode("append") \
      .save()


# 6. Write to snowflake
raw_query = deduped.writeStream \
    .foreachBatch(write_raw_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "spark/checkpoints/iot_raw") \
    .trigger(processingTime="5 seconds") \
    .start()

agg_query = batch_agg.writeStream \
    .foreachBatch(write_agg_to_snowflake) \
    .outputMode("append") \
    .option("checkpointLocation", "spark/checkpoints/iot_agg") \
    .trigger(processingTime="5 seconds") \
    .start()

raw_query.awaitTermination()
agg_query.awaitTermination()
