# Data Engineering for Hotel Booking Data APIs

Built end-to-end data engineering pipeline to extract, load, and transform Hotel Booking Data APIs from [RapidAPI site](https://rapidapi.com/tipsters/api/booking-com/)

<br>

👉 &nbsp; This repository contains a code to extract, load, and transform hotel booking data from the RapidAPI site using the below tech stacks.

<br>

## Data/Tech stack

- Extract via Airflow
- Transform via PySpark
- Load into intermediate Azure Blob Storage
- Azure Data Factory using lookup and the Databricks notebook activity
- Load into Databricks DeltaLakeHouse

### Pipeline Architecture

![alt text](https://github.com/abhijitmorye/DE-ELT_Hotel_Booking/blob/main/img/architecture.drawio.png?raw=true)

### Steps Performed

- Extracted Hotel Booking Data from Rapid API using Airflow PythonOperator and BashOperator in the form of JSON

- Before storing data in Azure Blob Storage, transform JSON data to convert it into CSV data files using PySpark and Airflow PythonOperator

- Once transformed, load CSV data into Azure Blob Storage.

- Developed Azure Data factory pipeline to load CSV data into Databricks DeltaLakeHouse in the form of delta tables.

- Before loading CSV data as delta tables, inside ADF, configured Databricks notebook to remove null values, and duplicate values as well as, built incremental data loading code using PySpark, databanks notebook and Spark Cluster.

- The whole process is orchestrated using the Airflow DAG scheduler, set to run daily.

- ADF data pipeline is set to trigger based on scheduled trigger daily.

### Airflow DAG

![alt text](https://github.com/abhijitmorye/DE-ELT_Hotel_Booking/blob/main/img/airflow_DAG.JPG?raw=true)

### ADF Pipeline

![alt text](https://github.com/abhijitmorye/DE-ELT_Hotel_Booking/blob/main/img/ADF_pipeline.JPG?raw=true)

### Databricks DeltaLakehouse

![alt text](https://github.com/abhijitmorye/DE-ELT_Hotel_Booking/blob/main/img/Deltalakehouse.JPG?raw=true)
