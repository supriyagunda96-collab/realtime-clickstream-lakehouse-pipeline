from pyspark.sql.functions import col, to_timestamp, date_format
from src.utils.spark_session import get_spark_session

spark = get_spark_session("Silver Clickstream Transformation")

bronze_df = (
    spark.read
    .parquet("lakehouse/bronze/clickstream_events")
)

silver_df = (
    bronze_df
    .dropDuplicates(["event_id"])
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isin("page_view", "product_view", "add_to_cart", "purchase"))
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
)

silver_df.write.mode("overwrite").partitionBy("event_date").parquet(
    "lakehouse/silver/clickstream_events"
)