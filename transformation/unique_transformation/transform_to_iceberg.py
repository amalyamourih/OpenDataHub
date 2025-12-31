from pyspark.sql import SparkSession

# AJUSTER les CLES

spark = SparkSession.builder \
    .appName("CSV_JSON_to_Parquet") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "acces_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "private_key") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()


s3_csv_path = "s3a://amzn-s3-opendatahub/tabular/csv/"
s3_tsv_path = "s3a://amzn-s3-opendatahub/tabular/tsv/"
s3_parquet_output = "s3a://amzn-s3-opendatahub/test/parquet/"

# Lecture des fichiers CSV et TSV
df_csv = spark.read.option("header", "true").csv(s3_csv_path)
df_tsv = spark.read.option("header", "true").option("sep", "\t").csv(s3_tsv_path)


# Sauvegarde au format Parquet sur S3
df_csv.write.mode("overwrite").parquet(s3_parquet_output + "csv/")
df_tsv.write.mode("overwrite").parquet(s3_parquet_output + "tsv/")

spark.stop()