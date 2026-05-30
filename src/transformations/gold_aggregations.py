from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum
from src.utils.spark_session import get_spark_session

spark = get_spark_session("Gold Product Metrics Aggregation")

silver_df = spark.read.parquet("lakehouse/silver/clickstream_events")

gold_df = (
    silver_df
    .groupBy("event_date", "product_id")
    .agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions"),
        count("*").alias("event_count"),
        spark_sum(
            col("price")
        ).alias("total_revenue")
    )
)

gold_df.write.mode("overwrite").partitionBy("event_date").parquet(
    "lakehouse/gold/product_metrics"
)