import threading
from pyspark.sql import SparkSession


class SparkSessionFactory:
    _thread_local = threading.local()

    @staticmethod
    def get_spark_session() -> SparkSession:
        if not hasattr(SparkSessionFactory._thread_local, "spark"):
            SparkSessionFactory._thread_local.spark = (SparkSession.builder
                                                       .appName("SodaQualityChecks")
                                                       .config("spark.executor.memory", "4g")
                                                       .config("spark.driver.memory", "2g")
                                                       .config("spark.local.dir", "./data")
                                                       .config("spark.ui.port", "4040")
                                                       .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
                                                       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                                                       .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
                                                       .getOrCreate())
        return SparkSessionFactory._thread_local.spark

    @staticmethod
    def stop_spark_session():
        if hasattr(SparkSessionFactory._thread_local, "spark"):
            SparkSessionFactory._thread_local.spark.stop()
            del SparkSessionFactory._thread_local.spark
