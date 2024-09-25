# AWS Lambda Data Pipeline Project

## Overview
This project implements a serverless data pipeline using **AWS Lambda** to automate the process of transferring and transforming data from **Amazon S3** into **Amazon Redshift**. The pipeline processes new insertions and updates, sends success or failure notifications via **Amazon SNS**, archives processed files, and connects to **Amazon QuickSight** for reporting, all within the same **VPC**.

## Architecture
![Project Architecture](https://github.com/kishoo-coder/AWS-Data-pipeline/blob/main/AWS%20Pipeline.jpg1) <!-- Replace with your image URL -->

The pipeline automates the following:
1. **Data Ingestion**: Fetches `.csv` files from specified folders in an S3 bucket.
2. **Data Processing**: 
   - Insert data into a staging table in Redshift.
   - Identify and insert new records into the production table.
   - Identify and update existing records in the production table.
3. **Archival**: Moves processed files to an archive folder.
4. **Notification**: Sends success or failure messages to an SNS topic.
5. **Visualization**: Connects to Amazon QuickSight for analytics and reporting.

## Features
- **S3 Integration**: Retrieves CSV files from an S3 bucket with folders for inserts and updates.
- **Redshift Serverless**: Inserts and updates data in staging and production tables.
- **Audit Log**: Tracks the number of inserted and updated records, along with timestamps.
- **File Archiving**: Archives processed files with a timestamp in the filename.
- **SNS Notifications**: Sends SNS notifications on success or failure of the pipeline.
- **QuickSight Integration**: Data visualization and reporting within the same VPC.
