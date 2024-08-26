# Q Company Data Platform

This repository contains the implementation of Q Company's data platform, which processes and analyzes retail and E-commerce data. The platform is designed to handle batch data ingestion and processing as well as real-time data streaming. The core technologies used include **Apache Kafka**, **Apache Spark**, and **Apache Hive**.

## Contributors
- Abdelrahman Ashraf
- Ahmed Atef
- Hussein Khaled
- Omar Ashraf

## Project Overview

Q Company operates across various regions with both physical retail branches and an E-commerce platform. The data platform supports the company's need to process and analyze data from multiple sources, providing insights to different business teams, including marketing and B2B operations.

<div align="center">
  <img src="https://github.com/user-attachments/assets/17c212d3-33bd-49f4-8c41-dc3aa6e1adc6" alt="Data Platform Diagram">
</div>

### Data Nature

- **Source Files**: The system pushes three types of files every hour:
  - Branches file
  - Sales agents file
  - Sales transactions file (includes both branch and online sales)

- **Shipping Address Schema**: In online sales transactions, the shipping address includes fields such as address, city, state, and postal code.

- **App Logs**: The company's app pushes logs to a Kafka cluster for later processing.

### Key Technologies

- **Apache Kafka**: Used for real-time log processing.
- **Apache Spark**: Employed for both batch and streaming data processing.
- **Apache Hive**: Acts as the data warehouse for storing processed data and supporting analytics.

## Batch Processing

### Technical Description

The batch processing pipeline handles hourly data ingestion and processing:

1. **File Ingestion**: 
   - Every hour, one group of files is placed in the local file system (LFS).
   - These files are ingested into the data lake as raw files using Python.
   - The data lake is partitioned to enable tracking of data over time.

2. **Data Processing**:
   - The raw files are processed using Spark to meet business requirements.
   - Cleaned and processed data is stored in Hive tables, forming the Data Warehouse (DWH).

3. **Data Warehouse (DWH) Modeling**:
   - The DWH is modeled to meet the needs of the business, focusing on transaction dates.
   - A `total_paid_price_after_discount` column is added to the fact table.

### Business Requirements

- **Marketing Insights**:
  - Identify the most selling products.
  - Determine the most redeemed offers by customers and per product.
  - Identify cities with the lowest online sales to target with campaigns.

- **B2B Reporting**:
  - A daily CSV dump is required containing `sales_agent_name`, `product_name`, and `total_sold_units`.
  - This file should be sent to the local file system.

## Streaming Processing

### Technical Description

The streaming pipeline processes real-time app logs sent to Kafka:

1. **Kafka Producer**:
   - A Python Kafka producer sends app logs with a dynamic schema to the Kafka cluster.
   - The producer can be started via a script or Jupyter notebook.

2. **Spark Streaming**:
   - A Spark streaming job consumes data from Kafka, processes it, and stores it on HDFS.

### Business Requirements

- Store the streaming data in a way that maximizes its value for the business.
- Provide at least two queries or reports based on the streaming data.

## Prerequisites

- Download Docker
- Download these Docker Image: https://github.com/itversity/spark-sql-and-pyspark-using-python3
- change the .yaml file with the attached one
- then run `docker compose up` command in terminal to setup the image.
  

Thank you for exploring the Q Company Data Platform. We hope this repository helps you understand how to build scalable data pipelines and derive actionable insights from your data. Happy coding!
