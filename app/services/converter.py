import json
import os
from datetime import datetime
from typing import List, Dict

from app.core.config import settings


# Load conversion configuration from a file
def load_conversion_config(config_path: str) -> Dict:
    """
    Loads the conversion configuration from a JSON file.

    Parameters:
    config_path (str): The path to the JSON file containing the conversion configuration.

    Returns:
    Dict: A dictionary representing the conversion configuration.

    The function opens the JSON file, reads its content, and returns the data as a dictionary.
    """

    with open(config_path, 'r') as config_file:
        return json.load(config_file)


def convert_spec_to_soda_cl(spec_path: str) -> str:
    """
    Converts a specification file in JSON format to a SodaCL (Soda Cloud Language) file.

    Parameters:
    spec_path (str): The path to the specification file in JSON format.

    Returns:
    str: The path to the generated SodaCL file.

    The function reads the specification data from the given JSON file, loads the conversion configuration,
    converts the specification data to SodaCL format, and writes the SodaCL content to a new file.
    The new file's name includes a timestamp to ensure uniqueness.
    """

    # Open and load the specification data from the JSON file
    with open(spec_path, 'r') as spec_file:
        spec_data = json.load(spec_file)

    # Load the conversion configuration from the specified path
    config_path = settings.config_path
    conversion_config = load_conversion_config(config_path)

    # Convert the specification data to SodaCL format
    soda_cl_content = convert_to_soda_cl(spec_data, conversion_config)

    # Generate a unique timestamp for the SodaCL file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Construct the path to the new SodaCL file
    soda_check_path = os.path.join(settings.soda_spec_dir, settings.soda_check_template.format(timestamp=timestamp))

    # Print the generated SodaCL content
    print("converted soda cl content:", soda_cl_content)

    # Write the SodaCL content to the new file
    with open(soda_check_path, 'w') as soda_file:
        soda_file.write(soda_cl_content)

    # Return the path to the generated SodaCL file
    return soda_check_path


def convert_to_soda_cl(spec_data: List[Dict], config: Dict) -> str:
    """
    This function converts a specification data into a SodaCL (Soda Cloud Language) format.

    Parameters:
    spec_data (List[Dict]): A list of dictionaries representing the specification data. Each dictionary contains details about a table.
    config (Dict): A dictionary containing the conversion configuration.

    Returns:
    str: A string representing the SodaCL content.

    The function iterates over the specification data and generates SodaCL checks based on the provided configuration.
    It adds default checks, default schema checks, and optional schema checks to the SodaCL content.
    """

    soda_cl_content = ""

    for spec in spec_data:
        for sheet in spec["details"]["sheets"]:
            table_name = sheet.get('table_name', 'default')  # Use 'default' if 'table_name' is not specified

            soda_cl_content += f"checks for {table_name}:\n"

            # Add default checks from configuration
            for default_check in config["default_checks"]:
                if "check" in default_check:
                    soda_cl_content += f"  - {default_check['check']}\n"

            # Add default schema checks
            if "default_schema" in config["schema_checks"]:
                for schema_check in config["schema_checks"]["default_schema"]:
                    soda_cl_content += "  - schema:\n"
                    if 'name' in schema_check:
                        soda_cl_content += f"      name: {schema_check['name']}\n"

                    if 'required_columns' in schema_check:
                        required_columns = schema_check['required_columns'] or [col['source_col_name'] for col in
                                                                                sheet['columns'] if
                                                                                col.get("is_required") == "Y"]
                        if required_columns:
                            soda_cl_content += f"      fail:\n"
                            soda_cl_content += f"        when required column missing: [{', '.join(required_columns)}]\n"

            # Add optional schema checks
            if "optional" in config["schema_checks"]:
                for schema_check in config["schema_checks"]["optional"]:
                    soda_cl_content += "  - schema:\n"
                    if 'name' in schema_check:
                        soda_cl_content += f"      name: {schema_check['name']}\n"

            soda_cl_content += "\n"

    return soda_cl_content
