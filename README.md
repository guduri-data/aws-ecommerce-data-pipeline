
# AWS E-commerce Batch Data Engineering Pipeline

## Overview
Built an end-to-end batch data pipeline on AWS using a Medallion Architecture (Raw → Silver → Gold).  
The pipeline ingests raw e-commerce data, processes it using Spark on AWS Glue, and exposes analytics-ready tables using Amazon Athena.

---

## Architecture
![Architecture Diagram](docs/architecture/architecture.png)

Raw data → S3 → Glue (Spark ETL) → Parquet (Silver) → Athena CTAS → Gold tables

---

## Tech Stack
- Amazon S3 (Data Lake)
- AWS Glue (Spark ETL, Crawlers)
- Amazon Athena (SQL Analytics)
- Apache Spark (PySpark)
- SQL

---

## Data Lake Structure
raw/ → source CSV files
silver/ → cleaned Parquet data
gold/ → analytics-ready tables
---

## Gold Tables
- dim_customers  
- dim_products  
- fact_orders  

---

## Sample Query
```sql
SELECT order_status, COUNT(*) 
FROM ecommerce_gold_db.fact_orders
GROUP BY order_status;
