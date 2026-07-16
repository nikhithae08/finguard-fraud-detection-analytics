from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(
    name = "finguard.gold.transaction_count_by_minute")

def transaction_count_by_minute() -> DataFrame:
    transactions_df = spark.readStream.table("finguard.silver.transactions")
    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")
    transaction_count_df = (
         transactions_with_watermark
         .groupBy(window("transaction_timestamp", "1 minute"))
         .agg(count("*").alias("transaction_count"))
         .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count")
         ))
    return transaction_count_df