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



# from pyspark.sql import SparkSession

# class SparkSessionFactory:
#     _spark_session = None

#     @classmethod
#     def get_spark_session(cls) -> SparkSession:
#         if cls._spark_session is None:
#             hadoop_conf = {
#                 "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375",
#                 "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
#                 "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
#                 "spark.hadoop.fs.s3a.access.key": "YOUR_ACCESS_KEY",
#                 "spark.hadoop.fs.s3a.secret.key": "YOUR_SECRET_KEY",
#                 "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com"
#             }

#             cls._spark_session = SparkSession.builder \
#                 .appName("YourAppName") \
#                 .config(conf=hadoop_conf) \
#                 .getOrCreate()
                
#             # For passing configurations to hadoop
#             hadoop_conf = cls._spark_session._jsc.hadoopConfiguration()
#             hadoop_conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
#             hadoop_conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY")
#             hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
#             hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#             hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")

#         return cls._spark_session

#     @classmethod
#     def stop_spark_session(cls):
#         if cls._spark_session is not None:
#             cls._spark_session.stop()
#             cls._spark_session = None






# from pyspark.sql import SparkSession
# import os

# class SparkSessionFactory:
#     _spark_session = None

#     @classmethod
#     def get_spark_session(cls) -> SparkSession:
#         if cls._spark_session is None:
#             cls._spark_session = SparkSession.builder \
#                 .appName("YourAppName") \
#                 .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
#                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#                 .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#                 .getOrCreate()
                
#             # For passing configurations to hadoop
#             hadoop_conf = cls._spark_session._jsc.hadoopConfiguration()
#             hadoop_conf.set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
#             hadoop_conf.set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
#             hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
#             hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#             hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")

#         return cls._spark_session

#     @classmethod
#     def stop_spark_session(cls):
#         if cls._spark_session is not None:
#             cls._spark_session.stop()
#             cls._spark_session = None

