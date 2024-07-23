import boto3
from pyspark.sql import SparkSession
from app.core.spark_session import SparkSessionFactory
from app.logger_config import logger

class S3Reader:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def read_csv_to_df(self, s3_path: str) -> SparkSession.DataFrame:
        logger.info(f"Reading CSV file from S3 path: {s3_path}")

        # Parse the S3 path to get the bucket name and key
        bucket, key = self._parse_s3_path(s3_path)
        
        # Create Spark session
        spark = SparkSessionFactory.get_spark_session()

        # Read the CSV file from S3 into a Spark DataFrame
        df = spark.read.csv(f"s3a://{bucket}/{key}", header=True, inferSchema=True)

        logger.debug(f"CSV file schema: {df.schema}")
        return df

    def _parse_s3_path(self, s3_path: str):
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path: {s3_path}")
        s3_path = s3_path[5:]  # Remove "s3://"
        bucket, key = s3_path.split('/', 1)
        return bucket, key
