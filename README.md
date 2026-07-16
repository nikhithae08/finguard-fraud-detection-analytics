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


## Technology Stack
<img width="709" height="461" alt="image" src="https://github.com/user-attachments/assets/341d4b83-965c-4952-9573-22f4092320e7" />


## Data Flow
1.  Credit card transactions are published to Kafka.
2.  Spark Structured Streaming ingests transaction events using Lakeflow Spark Declarative Pipelines.
3.  Fraud watchlist JSON files are loaded using Auto Loader into Lakeflow Spark Declarative Pipelines.
4.  Customer master data is incrementally ingested using Lakeflow Connect from Postgres SQL.
5.  Bronze stores raw data with ingestion_timestamp.
6.  Silver performs cleansing, standardization, schema validation and data quality checks.
7.  Gold applies fraud detection logic using joins, watermarking and window aggregations.
8.  Fraud alerts are sent through Gmail SMTP.
9.  Dashboards refreshes every minute.


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
-   GitHub Actions CI/CD for Databricks Asset Bundles

##  Skills Demonstrated
Databricks, Apache Spark, PySpark, Structured Streaming, Delta Lake, Lakeflow, Kafka, PostgreSQL, Unity Catalog, Auto Loader, CDC, SQL, Python, Window Aggregations, Streaming Joins, Data Engineering.

## Future Enhancements
-   Monitoring and observability
-   Automated testing

## Author
This project was developed as a portfolio project demonstrating modern Data Engineering practices for real-time analytics using Databricks Lakehouse.
