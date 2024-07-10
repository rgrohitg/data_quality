import json
import os
from datetime import datetime
from typing import List, Dict
from jinja2 import Template

from app.core.config import settings


def load_template(template_path: str) -> str:
    with open(template_path, 'r') as template_file:
        return template_file.read()


def convert_spec_to_soda_cl(spec_path: str, template_path: str) -> str:
    """
    Converts a specification file in JSON format to a SodaCL file.

    Parameters:
    spec_path (str): The path to the JSON specification file.
    template_path (str): The path to the SodaCL template file.

    Returns:
    str: The path to the generated SodaCL file.
    """
    with open(spec_path, 'r') as spec_file:
        spec_data = json.load(spec_file)

    template_content = load_template(template_path)
    soda_cl_content = convert_to_soda_cl(spec_data, template_content)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    soda_check_path = os.path.join(settings.soda_spec_dir, settings.soda_check_template.format(timestamp=timestamp))

    print("converted soda cl content:", soda_cl_content)

    with open(soda_check_path, 'w') as soda_file:
        soda_file.write(soda_cl_content)

    return soda_check_path


def map_json_type_to_soda_type(json_type: str) -> str:
    """
    Maps a JSON data type to a corresponding SodaCL data type.

    Parameters:
    json_type (str): The JSON data type to be mapped.

    Returns:
    str: The corresponding SodaCL data type. If the JSON data type is not found in the mapping,
    it returns "string" as a default.
    """
    type_mappings = {
        "text": "string",
        "decimal": "decimal(10,0)",
        "integer": "integer",
        "double": "double",
        # Add other type mappings as needed
    }
    return type_mappings.get(json_type.lower(), "string")


def generate_default_checks(template_content: str) -> str:
    template = Template(template_content)
    return template.render() + "\n"


def generate_missing_count_checks(columns: List[Dict], template_content: str) -> str:
    """
    Generates missing count checks for required columns in a SodaCL specification.

    Parameters:
    columns (List[Dict]): A list of dictionaries, where each dictionary represents a column in the dataset.
        Each dictionary should contain the keys 'source_col_name' and 'is_required'.
    template_content (str): The content of the SodaCL template, which contains a placeholder for the column name.

    Returns:
    str: A string containing the missing count checks for required columns, formatted according to the SodaCL template.
    """
    template = Template(template_content)
    missing_count_checks = ""
    for column in columns:
        if column.get("is_required") == "Y":
            col_name = column['source_col_name']
            missing_count_checks += template.render(col=col_name) + "\n"
    return missing_count_checks


def generate_schema_checks(columns: List[Dict], template_content: str) -> str:
    """
    Generates schema checks for required columns and wrong column types in a SodaCL specification.

    Parameters:
    columns (List[Dict]): A list of dictionaries, where each dictionary represents a column in the dataset.
        Each dictionary should contain the keys 'source_col_name' and 'source_col_type'.
    template_content (str): The content of the SodaCL template, which contains placeholders for the column name and types.

    Returns:
    str: A string containing the schema checks for required columns and wrong column types, formatted according to the SodaCL template.
    """
    template = Template(template_content)
    required_columns = [col['source_col_name'] for col in columns if col.get("is_required") == "Y"]
    wrong_column_types = "\n".join(
        [f"          {col['source_col_name']}: {map_json_type_to_soda_type(col['source_col_type'])}" for col in columns]
    )
    return template.render(required_columns=", ".join(required_columns), wrong_column_types=wrong_column_types) + "\n"


def convert_to_soda_cl(spec_data: List[Dict], template_content: str) -> str:
    """
    Converts a list of specifications in JSON format to a SodaCL file content.

    Parameters:
    spec_data (List[Dict]): A list of dictionaries, where each dictionary represents a specification.
        Each specification should contain a 'details' key with a 'sheets' key, which is a list of sheets.
        Each sheet should contain 'columns' key, which is a list of dictionaries representing columns.
    template_content (str): The content of the SodaCL template, which contains placeholders for different checks.

    Returns:
    str: The content of the generated SodaCL file, containing checks for each sheet in the specifications.
    """
    soda_cl_content = ""

    for spec in spec_data:
        for sheet in spec["details"]["sheets"]:
            table_name = sheet.get('table_name', 'default')
            soda_cl_content += f"checks for {table_name}:\n"

            # Split template content into different sections
            template_parts = template_content.split("\n\n")
            default_checks_template = template_parts[0]
            missing_count_checks_template = template_parts[1]
            schema_checks_template = template_parts[2]

            # Add default checks
            soda_cl_content += generate_default_checks(default_checks_template)

            # Add missing count checks for required columns
            soda_cl_content += generate_missing_count_checks(sheet['columns'], missing_count_checks_template)

            # Add schema checks
            soda_cl_content += generate_schema_checks(sheet['columns'], schema_checks_template)

            soda_cl_content += "\n"

    return soda_cl_content.strip()  # Remove the trailing newline

#
# # Example usage
# if __name__ == "__main__":
#     spec_path = "path/to/your/specification.json"
#     template_path = "path/to/your/soda_checks_template.yml"
#     convert_spec_to_soda_cl(spec_path, template_path)
