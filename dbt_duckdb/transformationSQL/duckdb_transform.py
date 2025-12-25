import boto3
import pandas as pd
from io import BytesIO
import duckdb

# Initialize S3 client
s3_client = boto3.client('s3')

# TODO : Make this code more generic and adapted to asked architecture
BUCKET_NAME = 'amzn-s3-opendatahub'
S3_KEY = 'tabular/csv/export-harvest-20251130-060624.csv'


def check_aws_credentials():
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print("AWS credentials are valid.")
        print(f"Account: {identity['Account']}, ARN: {identity['Arn']}")
    except Exception as e:
        print(f"Error with AWS credentials: {str(e)}")


def load_data_from_s3(bucket_name, s3_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        body = response["Body"].read()

        if s3_key.endswith(".csv"):
            df = pd.read_csv(BytesIO(body), sep=None, engine="python")
        elif s3_key.endswith(".parquet"):
            df = pd.read_parquet(BytesIO(body))
        elif s3_key.endswith(".json"):
            df = pd.read_json(BytesIO(body))
        else:
            raise ValueError(f"Unsupported file format: {s3_key}")

        print(f"Successfully loaded {len(df)} rows.")
        return df

    except Exception as e:
        print(f"Error loading data from S3: {e}")
        raise

def df_to_duckdb(df, table_name="harvest_data", db_path="dbt_duckdb\\transformationSQL\\data_temp\\data.duckdb"):
    conn = duckdb.connect(database=db_path)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    conn.close()
    print(f"Table '{table_name}' saved in {db_path}")
    return conn


if __name__ == "__main__":
    check_aws_credentials()
    df = load_data_from_s3(BUCKET_NAME, S3_KEY)
    print(df.head())
    duckdb_conn = df_to_duckdb(df)
