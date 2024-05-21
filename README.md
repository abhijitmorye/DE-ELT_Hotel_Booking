# Data Engineering for Hotel Booking Data APIs

Built end-to-end data engineering pipeline to extract, load and transform Hotel Booking Data APIs from [RapidAPI site](https://rapidapi.com/tipsters/api/booking-com/)

<br>

👉 &nbsp; This repository contains a code to exxtract, load and transform hotel booking data from RapidAPI site using below tech stacks.

<br>

## Data/Tech stack

- [Set up](#setup)
- [Extract via Airflow](#extract-via-airbyte)
- [Transform via PySpark](#transform-via-dbt-cloud)
- [Load into Databricks DeltaLakeHouse](https://www.databricks.com/product/data-lakehouse)

### Pipeline Architecture

### Steps Performed

- Extracted Hotel Booking Data from Rapid API using Airflow PythonOperator and BashOperator in the form of JSON

- Before storing data into Azure Blob Storage, transform JSON data to convert it into CSV data files using PySpark and Airflow PythonOperator

- Once transformed, load CSV data into Azure Blob Storage.

- Developed Azure Data factor pipeline to load CSV data into Databricks DeltaLakeHouse in the form of delta tables.

- Before loading CSV data as delta tables, inside ADF, configured databricks notebook to remove null values, duplicate values as well as, built incremental data loading code using PySpark, databcirks notebook and Spark Cluster.

- Whole processe is orchastrated using Airflow DAG scheduler, set to run daily.

- ADF data pipeline is set to trigger based on scheduled trigger daily.

### Airflow DAG

### ADF Pipeline

### Databricks DeltaLakehouse
