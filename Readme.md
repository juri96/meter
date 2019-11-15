# Meter

This project contains example serverless ETL pipeline, which loads data from CSV files into a BigQuery dataset and 
runs K-means clustering on loaded data

Dataset: https://archive.ics.uci.edu/ml/datasets/Beijing+Multi-Site+Air-Quality+Data

## Build
Windows: `gradlew clean build`

Linux: `./gradlew clean build`

## Run
`java -jar ./build/libs/meter-1.0-SNAPSHOT.jar
--runner
DataflowRunner
--project
GCP_PROJECT_NAME
--input
gs://GCP_BUCKET_NAME/*.csv
--job_name
electricmeters-import
--dataset
BIGQUERY_DATASET_NAME
--temp_location
gs://GCP_BUCKET_NAME/temp
--staging_location
gs://GCP_BUCKET_NAME/staging
--table_name
measurements
`