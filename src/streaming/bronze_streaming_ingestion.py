from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.utils.spark_session import get_spark_session

spark = get_spark_session("Bronze Clickstream Streaming Ingestion")

schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("device_type", StringType()),
    StructField("event_time", StringType()),
    StructField("price", DoubleType())
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail_clickstream_events")
    .option("startingOffsets", "latest")
    .load()
)

bronze_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingestion_timestamp", current_timestamp())
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .option("path", "lakehouse/bronze/clickstream_events")
    .option("checkpointLocation", "checkpoints/bronze")
    .outputMode("append")
    .start()
)

query.awaitTermination()