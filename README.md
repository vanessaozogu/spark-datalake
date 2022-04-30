## INTRODUCTION
Sparkify, a music streaming service, has expanded its user base and song collection even further, and plans to transfer its data center to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the site, as well as a directory on the songs in their site with JSON metadata. The aim of this project is to build a data lake and an ETL pipeline in Spark that loads data from S3, processes the data into analytics tables, and loads them back into S3.

## HOW TO RUN THIS PROJECT
1. Create an S3 Bucket named results to store the output data
2. Enter your Access and Secret keys in the dl.cfg file
3. Type "python etl.py" in the terminal and press enter.

## FILES DESCRIPTION
dl.cfg: Will contain the Access Keys and Secret keys
etl.py: This script extracts songs and log data from S3, transforms it using Spark, and loads parquet-format dimensional tables back into S3
