# AWS Glue: CSV to Avro ETL

## Overview
This project reads a CSV file from Amazon S3, uses AWS Glue ETL to transform it into Avro format, and writes it back to S3.

## Steps
1. Upload `sample_data.csv` to an S3 bucket (`s3://my-bucket/input/`).
2. Create a Glue Crawler to catalog the CSV file.
3. Create a Glue Job with Visual ETL (code from visual ETL glue_etl_job_orders.py).
4. Create IAM role and add permissions for the job.
5. Create Lambda orderGlueLambda and add code lambda_functions.py and setup permissions.
6. Run the job and verify Avro files in the output S3 path.

## Architecture
S3 → Glue Crawler → Glue ETL Job → Lamdba → S3
