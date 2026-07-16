from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(
    name = "finguard.gold.fraud_card_alert",
    comment = "Alert details for transactions due to fraud card")

def fraud_card_alert() -> DataFrame:
    transactions_df = spark.readStream.table("finguard.silver.transactions")
    fraud_watchlist_df = spark.readStream.table("finguard.silver.fraud_watchlist")

    customers_df = spark.read.table("finguard.silver.customers")

    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")
    fraud_watchlist_with_watermark = fraud_watchlist_df.withWatermark("effective_from", "5 minutes")

    fraud_detected = (
            transactions_with_watermark
            .join(fraud_watchlist_with_watermark,
                  transactions_with_watermark.card_number == fraud_watchlist_with_watermark.entity_id,"inner")
            .join(customers_df, transactions_with_watermark.customer_id == customers_df.customer_id, "left")
            .select(
            # Alert identification
            concat_ws("-", lit("FRAUD"), col("transaction_id"), col("watchlist_id")).alias("alert_id"),
            lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),
            
            # Transaction details
            transactions_with_watermark.transaction_id,
            transactions_with_watermark.customer_id,
            customers_df.email.alias("customer_email"),
            concat_ws(" ", customers_df.first_name, customers_df.last_name).alias("customer_name"),
            transactions_with_watermark.card_number,
            transactions_with_watermark.amount,
            transactions_with_watermark.currency,
            transactions_with_watermark.merchant_id,
            transactions_with_watermark.merchant_name,
            transactions_with_watermark.merchant_category,
            transactions_with_watermark.transaction_type,
            transactions_with_watermark.payment_channel,
            transactions_with_watermark.device_id,
            transactions_with_watermark.city.alias("transaction_city"),
            transactions_with_watermark.country.alias("transaction_country"),
            transactions_with_watermark.transaction_timestamp,
            transactions_with_watermark.is_international,
            transactions_with_watermark.status.alias("transaction_status"),
            
            # Fraud watchlist details
            col("watchlist_id"),
            col("watch_type"),
            col("risk_level"),
            col("action"),
            col("reason_code"),
            col("reason_description"),
            col("effective_from").alias("watchlist_effective_from"),
            col("reported_by"),
            col("reported_source"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")
        ))
    
    return fraud_detected