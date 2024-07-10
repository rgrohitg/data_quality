import threading
from pyspark.sql import SparkSession

# Thread-local storage for Spark session
thread_local = threading.local()


def get_spark_session() -> SparkSession:
    if not hasattr(thread_local, "spark"):
        thread_local.spark = (SparkSession.builder
                              .appName("SodaQualityChecks")
                              .config("spark.executor.memory", "4g")
                              .config("spark.driver.memory", "2g")
                              .config("spark.local.dir", "./data")
                              .config("spark.ui.port", "4040")  # Specify the port for Spark UI
                              .getOrCreate())
    return thread_local.spark


def stop_spark_session():
    if hasattr(thread_local, "spark"):
        thread_local.spark.stop()
        del thread_local.spark
