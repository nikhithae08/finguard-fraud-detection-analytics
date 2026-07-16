# finguard-fraud-detection-analytics
## Databricks | Spark Streaming | Kafka | Delta Lake | Lakeflow | Unity Catalog
Enterprise Real-Time Fraud Detection Platform built using Azure Databricks, Lakeflow, Kafka, Delta Lake, and Unity Catalog.

## Project Overview
FinGuard is a real-time credit card fraud detection platform built on Databricks Lakehouse architecture.
It processes streaming credit card transactions from Kafka, enriches them with customer and fraud reference data, detects suspicious patterns using Spark Structured Streaming, and generates real-time fraud alerts.

## Business Problem
Traditional fraud detection systems rely on batch processing,causing delayed detection.
FinGuard solves this by providing:
- real-time transaction monitoring
- automated fraud detection
- instant alert generation
- operational dashboards

## Project Outcomes
- Enabled continuous ingestion of streaming transactions.
- Detected suspicious transactions within seconds.
- Automated fraud notifications.
- Built an end-to-end Lakehouse architecture.
- Demonstrated modern streaming patterns including joins, watermarking and window aggregations.

## Architecture Diagram
<img width="4880" height="3280" alt="image" src="https://github.com/user-attachments/assets/83c2057b-f1ff-4a99-b40d-6a24ad0bae72" />
Streaming transactions from Kafka (via Spark Streaming) and fraud-watchlist files (via Auto Loader), together with batch customer data from PostgreSQL (via Lakeflow Connect), flow through Bronze → Silver → Gold layers built with Lakeflow Declarative Pipelines. Everything is governed by Unity Catalog, orchestrated by Lakeflow Jobs, and consumed through dashboards and real-time email alerts.

## Technology Stack
<img width="709" height="461" alt="image" src="https://github.com/user-attachments/assets/341d4b83-965c-4952-9573-22f4092320e7" />

## Key Features
-   Real-time transaction ingestion from Kafka
-   Incremental customer ingestion from PostgreSQL using CDC
-   Auto Loader for fraud watchlist ingestion
-   Bronze--Silver--Gold Medallion Architecture
-   Stream--Static and Stream--Stream joins
-   Watermarking, tumbling and sliding window aggregations
-   Declarative data quality expectations
-   Real-time fraud alerts using Gmail SMTP
-   Near real-time operational dashboard
-   Unity Catalog governance
-   Dashboard creation using Genie

## Repository Structure
finguard-fraud-detection-analytics
|
|README.md
├── databricks
│   ├── notebooks
│   └── finguard_dab
├── docs
├── sql
└── deployment

## Data Flow
1.  Credit card transactions are published to Kafka.
2.  Spark Structured Streaming ingests transaction events using Lakeflow Spark Declarative Pipelines.
3.  Fraud watchlist JSON files are loaded using Auto Loader into Lakeflow Spark Declarative Pipelines.
4.  Customer master data is incrementally ingested using Lakeflow Connect from PostgresSQL.
5.  Bronze stores raw data with ingestion_timestamp.
6.  Silver performs cleansing, standardization, schema validation and data quality checks.
7.  Gold applies fraud detection logic using joins, watermarking and window aggregations.
8.  Fraud alerts are sent through Gmail SMTP.
9.  Dashboards refresh every minute.


## Data Source
FinGuard combines three different data sources — two streaming and one batch.

#### Confluent Kafka — Live Transactions (Streaming)
Live credit card transactions are continuously published to a Confluent Kafka topic (`credit_card_transactions`) and streamed into Databricks for real-time processing.
#### JSON Files — Fraud Watchlist (Streaming)
A generator continuously produces JSON files and places them into a Databricks Volume, with each file representing a fraud watchlist event.
These records contain information about cards or entities flagged by internal fraud teams or external partners (such as phishing-related card blocks). The data is ingested continuously using Auto Loader to ensure the latest watchlist updates are available for streaming pipelines.
#### Neon PostgreSQL — Customer Master Data (Batch)
A hosted Neon PostgreSQL database serves as the source for customer master data, containing customer profile and reference information, including transaction limits used during fraud detection.
As this dataset changes infrequently, it is ingested into Databricks using Lakeflow Connect as a batch/incremental load, leveraging a primary key and cursor column to capture only new or updated records.

## Data Pipelines

The solution follows the **Medallion Architecture** within the **finguard** Unity Catalog:

* **Bronze:** Raw data ingestion
* **Silver:** Data cleaning, standardization, and validation
* **Gold:** Business-ready datasets for fraud detection and analytics

### Bronze Layer

| Table                             | Source                         | Ingestion                            |
| --------------------------------- | ------------------------------ | ------------------------------------ |
| `finguard.bronze.transactions`    | Confluent Kafka                | Spark Structured Streaming           |
| `finguard.bronze.fraud_watchlist` | JSON Files (Databricks Volume) | Auto Loader                          |
| `finguard.bronze.customers`       | Neon PostgreSQL                | Lakeflow Connect (Batch/Incremental) |

---

### Silver Layer

The Silver layer transforms Bronze data using **Lakeflow Declarative Pipelines** and applies data quality expectations.

| Silver Table                      |                         Purpose                                          |
|-----------------------------------|--------------------------------------------------------------------------|
| `finguard.silver.transactions`    | Parse transaction JSON, standardize schema, and validate critical fields |
| `finguard.silver.fraud_watchlist` | Standardize watchlist data and clean records                             |
| `finguard.silver.customers`       | Standardize customer data and validate customer IDs                      |

> Customer data is batch-ingested but processed as a streaming table for incremental updates.

---

### Gold Layer

The Gold layer produces real-time fraud alerts and streaming analytics.

| Gold Table                                   | Purpose                                          |
| -------------------------------------------- | ------------------------------------------------ |
| `high_value_transactions_alert`              | Detect transactions exceeding customer limits    |
| `fraud_card_alert`                           | Detect transactions matching the fraud watchlist |
| `transaction_count_by_minute`                | 1-minute tumbling window aggregation             |
| `transaction_count_by_minute_sliding_window` | 5-minute sliding window aggregation              |

---

## Real-Time Email Alerts

Streaming alerts automatically trigger HTML email notifications using **`@append_flow`** and **`@foreach_batch_sink`**, with Gmail SMTP credentials securely stored in the Databricks Secret Scope.

* **High-Value Transaction Alert:** Sent when a transaction exceeds the customer's configured limit.
  <img width="690" height="602" alt="image" src="https://github.com/user-attachments/assets/3715a334-05bc-4d22-b120-d29d2d2cf2b2" />

* **Fraud Card Alert:** Sent when a transaction matches the fraud watchlist.
  <img width="1174" height="660" alt="image" src="https://github.com/user-attachments/assets/3a464798-f8bd-4ca8-8e78-4c5476f740bd" />

> **Note:** Emails are delivered via Gmail SMTP and may initially appear in the Spam folder.

## Dashboard

The **FinGuard Fraud Detection Monitoring** dashboard provides near-real-time insights into transaction activity, fraud alerts, and streaming metrics. Powered by the Silver and Gold layers, it refreshes every minute for continuous operational monitoring.



