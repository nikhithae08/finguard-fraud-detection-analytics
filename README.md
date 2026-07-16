# finguard-fraud-detection-analytics
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
finguard-fraud-detection-analytics/
├── README.md
├── databricks/
│   ├── notebooks/
│   └── finguard_dab/
├── docs/
├── data/
├── sql/
├── deployment/
└── .github/

## Data Flow
1.  Credit card transactions are published to Kafka.
2.  Spark Structured Streaming ingests transaction events using Lakeflow Spark Declarative Pipelines.
3.  Fraud watchlist JSON files are loaded using Auto Loader into Lakeflow Spark Declarative Pipelines.
4.  Customer master data is incrementally ingested using Lakeflow Connect from Postgres SQL.
5.  Bronze stores raw data with ingestion_timestamp.
6.  Silver performs cleansing, standardization, schema validation and data quality checks.
7.  Gold applies fraud detection logic using joins, watermarking and window aggregations.
8.  Fraud alerts are sent through Gmail SMTP.
9.  Dashboards refresh every minute.


## Core Engineering Concepts
-   Event-driven streaming
-   Incremental CDC ingestion
-   Stateful and stateless processing
-   Checkpointing and fault tolerance
-   Delta Lake ACID transactions
-   Medallion Architecture
-   Data quality enforcement
-   Secure secret management
-   Workflow orchestration
-   Dashboard creation with Genie
-   GitHub Actions CI/CD for Databricks Asset Bundles

##  Skills Demonstrated
Databricks, Apache Spark, PySpark, Structured Streaming, Delta Lake, Lakeflow Connect, Lakeflow Spark Declarative Pipelines, Lakeflow Connect, Kafka, PostgreSQL, Unity Catalog, Auto Loader, CDC, SQL, Python, Window Aggregations, Streaming Joins, Data Engineering, Genie.

## Data Source
FinGuard combines three different data sources — two streaming and one batch.

### Confluent Kafka — Live Transactions (Streaming)
Live credit card transactions are continuously published to a Confluent Kafka topic (`credit_card_transactions`) and streamed into Databricks for real-time processing.
### JSON Files — Fraud Watchlist (Streaming)
A generator continuously produces JSON files and places them into a Databricks Volume, with each file representing a fraud watchlist event.
These records contain information about cards or entities flagged by internal fraud teams or external partners (such as phishing-related card blocks). The data is ingested continuously using Auto Loader to ensure the latest watchlist updates are available for streaming pipelines.
### Neon PostgreSQL — Customer Master Data (Batch)
A hosted Neon PostgreSQL database serves as the source for customer master data, containing customer profile and reference information, including transaction limits used during fraud detection.
As this dataset changes infrequently, it is ingested into Databricks using Lakeflow Connect as a batch/incremental load, leveraging a primary key and cursor column to capture only new or updated records.

## Future Enhancements
-   Monitoring and observability
-   Automated testing

## Author
This project was developed as a portfolio project demonstrating modern Data Engineering practices for real-time analytics using Databricks Lakehouse.
