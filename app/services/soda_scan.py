import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import lit, monotonically_increasing_id
from soda.scan import Scan
from soda.common.logs import configure_logging
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext
from app.models.models import ValidationResult
from app.logger_config import logger

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
        self.failed_rows = []

    def store_sample(self, sample_context: SampleContext):
        rows = sample_context.sample.get_rows()
        if rows:
            schema = T.StructType(
                [T.StructField(name=col.name, dataType=SODA_TO_PYSPARK_TYPE_MAP[col.type])
                 for col in sample_context.sample.get_schema().columns
                 ]
            )
            print("schema schema:", schema)

            # Add check_name to the schema
            schema.add(T.StructField("check_name", T.StringType(), True))

            # Append check_name to each row
            check_name = sample_context.check_name
            extended_rows = [tuple(row) + (check_name,) for row in rows]
            print("CHECK NAME:", check_name)

            # Create DataFrame
            self.retrieved_df = self._spark.createDataFrame(extended_rows, schema)
            self.retrieved_df.show(truncate=False)  # Display DataFrame for debugging

            columns = [field.name for field in schema.fields]
            for row in extended_rows:
                failed_row = {columns[i]: value for i, value in enumerate(row)}
                self.failed_rows.append(failed_row)
        else:
            logger.info("No rows found in the sample.")

    def get_retrieved_df(self):
        return self.retrieved_df

    def get_failed_rows(self):
        return self.failed_rows


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

    return {
        "scan_results": scan.get_scan_results(),
        "failed_rows": custom_sampler.get_failed_rows()
    }

def format_response(scan_results, failed_rows, dynamoDbKey, templateName, fileName):
    errors = {}
    success = {}

    print("******************************************FORMAT RESPONSE FUNCTION****************************************************")

    # Extract checks from scan results
    checks = scan_results.get('checks', [])

    # Map to keep track of check results
    check_result_map = {check['name']: check for check in checks}

    # Populate the errors dictionary
    for row in failed_rows:
        row_index = row.get("ROW")
        check_name = row.get("check_name")
        check_result = check_result_map.get(check_name)

        if check_result:
            column_name = check_result.get("column", "")
            if check_result["outcome"] == "fail":
                if check_name not in errors:
                    errors[check_name] = []
                error_message = f"[ROW_{row_index}][COLUMN_{column_name}] Failed"
                errors[check_name].append(error_message)

    # Populate the success dictionary
    for check in checks:
        check_name = check["name"]
        if check["outcome"] == "pass":
            passed_rows_count = check["diagnostics"].get("value", "N/A")
            success[check_name] = f"Records/Check Value: {passed_rows_count}"

    response = {
        "status": "success",
        "results": {
            "summary": "validation completed successfully",
            "validation_results": [
                {
                    "file_details": {
                        "dynamoDbKey": dynamoDbKey,
                        "templateName": templateName,
                        "fileName": fileName,
                    },
                    "errors": errors,
                    "success": success
                }
            ]
        }
    }

    return response

