from typing import Dict

from pyspark.sql import SparkSession
from soda.scan import Scan
from soda.common.logs import configure_logging
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext


class CustomSampler(Sampler):
    def __init__(self):
        self.errors: dict = {}
        self.success: dict = {}

    def store_sample(self, sample_context: SampleContext):
        rows = sample_context.sample.get_rows()
        print("FAILED ROWS:{rows}", rows)
        if rows:
            self.errors[sample_context.check_name] = {
                "schema": sample_context.sample.get_schema(),
                "rows": rows
            }
        else:
            self.success[sample_context.query] = "Passed"


def configure_and_execute_scan(spark: SparkSession, soda_check_path: str, custom_sampler: CustomSampler) -> dict:
    configure_logging()
    scan = Scan()
    scan.sampler = custom_sampler
    scan.set_scan_definition_name("CSV_FILE_TEST")
    scan.set_data_source_name(data_source_name="spark_df")
    scan.add_spark_session(spark_session=spark)
    scan.add_sodacl_yaml_file(file_path="./data/soda_check.yml")

    scan.set_verbose(True)
    scan.execute()
    print("SCAN RESULTS :", scan.scan_results)
    return scan.get_scan_results()
