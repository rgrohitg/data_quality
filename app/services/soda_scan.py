import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import lit, monotonically_increasing_id
from soda.scan import Scan
from soda.common.logs import configure_logging
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext
from app.models.models import ValidationResult
from app.logger_config import logger

# Create a mapping from Soda schema types to PySpark SQL types
SODA_TO_PYSPARK_TYPE_MAP = {
    'string': T.StringType(),
    'integer': T.IntegerType(),
    'long': T.LongType(),
    'float': T.FloatType(),
    'double': T.DoubleType(),
    'boolean': T.BooleanType(),
    'StringType': T.StringType(),
    'IntegerType': T.IntegerType(),
    'LongType': T.LongType(),
    'FloatType': T.FloatType(),
    'DoubleType': T.DoubleType(),
    'BooleanType': T.BooleanType(),
    # Add more types as needed
}

class CustomSampler(Sampler):
    def __init__(self, spark: SparkSession):
        super().__init__()
        self._spark = spark
        self.retrieved_df = None

    def store_sample(self, sample_context: SampleContext):
        rows = sample_context.sample.get_rows()
        if rows:
            schema = T.StructType(
                [ T.StructField(name=col.name, dataType=SODA_TO_PYSPARK_TYPE_MAP[col.type])
                  for col in sample_context.sample.get_schema().columns
                  ]
            )

            # Create DataFrame
            self.retrieved_df = self._spark.createDataFrame(rows, schema)
            self.retrieved_df.show(truncate=False)  # Display DataFrame for debugging
        else:
            logger.info("No rows found in the sample.")

    def get_retrieved_df(self):
        return self.retrieved_df

def configure_and_execute_scan(spark: SparkSession, soda_check_path: str, custom_sampler: CustomSampler, spec_file_path: str, data_file_path: str):
    configure_logging()
    scan = Scan()
    scan.sampler = custom_sampler
    scan.set_scan_definition_name("CSV_FILE_TEST")
    scan.set_data_source_name(data_source_name="spark_df")
    scan.add_spark_session(spark_session=spark)
    scan.add_sodacl_yaml_file(file_path=soda_check_path)

    scan.set_verbose(True)
    scan.execute()

    # Retrieve and return the DataFrame
    return custom_sampler.get_retrieved_df()
