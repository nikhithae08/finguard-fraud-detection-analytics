from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(
    name = "finguard.gold.high_value_transactions_alert",
    comment = "Alert details for transactions higher than customer's transaction limit")

def high_value_transactions_alert() -> DataFrame:
    transactions_df = spark.readStream.table("finguard.silver.transactions")
    customers_df = spark.read.table("finguard.silver.customers")
    joined_df = (transactions_df.join(customers_df,transactions_df.customer_id == customers_df.customer_id,"left")
        .filter(col("amount") > col("transaction_limit"))
        .select(
            concat_ws("-",lit("ALERT"),col("transaction_id")).alias("alert_id"),
            lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
            current_timestamp().alias("alert_timestamp"),
            "transactions.transaction_id",
            "transactions.customer_id",
            col("customers.email").alias("customer_email"),
            concat_ws(" ",col("first_name"),col("last_name")).alias("customer_name"),
            col("transactions.amount").alias("transaction_amount"),
            "customers.transaction_limit",
            "transactions.currency",
            "transactions.merchant_name",
            "transactions.merchant_category",
            "transactions.transaction_type",
            "transactions.payment_channel",
            "transactions.city",
            "transactions.country",
            "transactions.is_international",
            "transactions.status",
            "transactions.transaction_timestamp"           
        ))
    return joined_df


    