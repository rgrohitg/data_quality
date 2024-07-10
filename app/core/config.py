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
    """The directory path where Soda spec files are located."""

    soda_check_template: str = "soda_check_{timestamp}.yml"
    """The template for creating Soda check files."""

    data_file_path: str = "./data/data.csv"
    """The file path of the data file to be validated."""

    config_path: str = "./data/conversion_config.json"
    """The file path of the configuration file."""


settings = Settings()
