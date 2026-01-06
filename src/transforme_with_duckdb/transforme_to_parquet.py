from pyspark.sql import SparkSession
from src.ingestion_to_S3.config import S3_BUCKET
import pandas as pd
import logging
import sys 
import os 

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("Transforme to Parquet") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ) \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
    ) \
    .getOrCreate()

def convert_csv_to_parquet(csv_file , parquet_file):
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    #df.write.parquet(parquet_file)

def convert_tsv_to_parquet(tsv_file , parquet_file):
    df = spark.read.csv(tsv_file, header=True, inferSchema=True, sep='\t')
    #df.write.parquet(parquet_file)

def convert_excel_to_parquet(excel_file , parquet_file):
    
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dataAddress", "'Sheet1'!A1") \
        .load(excel_file)
    
    #df.write.parquet(parquet_file)

def convert_parquet_to_parquet(parquet_input_file , parquet_output_file):
    df = spark.read.parquet(parquet_input_file)
    #df.write.parquet(parquet_output_file)       



if __name__ == "__main__":
    path_csv_file = "tabular/csv/dim_customer.csv"
    key = f"s3a://{S3_BUCKET}/{path_csv_file}"
    output_parquet_file = "/tmp/dim_customer.parquet"
    convert_csv_to_parquet(key , output_parquet_file)
    print(f"Conversion de {key} vers terminée.")

