from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(
    name = "finguard.bronze.fraud_watchlist",
    comment = "Fraud Watchlist data ingested by Autoloader")

def fraud_watchlist_bronze() -> DataFrame:

    streaming_df =( spark.readStream.format("cloudFiles")
               .option("cloudFiles.format","json")
               .option("cloudFiles.inferColumnTypes", "true")
               .load("/Volumes/finguard/source/fraud_watchlist/source_data/"))
    
    parsed_streaming_df = streaming_df.select(
        col("watchlist_id"),
        col("watch_type"),
        col("entity_id"),
        col("risk_level"),
        col("action"),
        col("reason_code"),
        col("reason_description"),
        col("status"),
        col("effective_from"),
        col("reported_by"),
        col("reported_source"),
        col("country"),
        col("city"),
        col("_rescued_data"),
        col("_metadata.file_path").alias("source_file"),
        current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df 

    
    
    
    
    
    

