#config.py
# File Path app/core/
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    A class to hold application settings.

    Attributes:
    soda_spec_dir (str): The directory path where Soda spec files are located.
    soda_check_template (str): The template for creating Soda check files.
    data_file_path (str): The file path of the data file to be validated.
    config_path (str): The file path of the configuration file.
    """

    soda_spec_dir: str = "./data"
    soda_check_template: str = "soda_check_{sheet_name}_{timestamp}.yml"
    data_file_path: str = "./data/data.csv"
    config_path: str = "./data/soda_conversion_template.yml"
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "your-access-key")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "your-secret-key")
    aws_region_name: str = os.getenv("AWS_REGION", "your-region")
    dynamodb_table_name: str = os.getenv("DYNAMODB_TABLE_NAME", "your-dynamodb-table")
    s3_bucket_name: str = os.getenv("S3_BUCKET_NAME", "your-s3-bucket")

settings = Settings()
