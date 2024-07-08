import json
import os
from datetime import datetime
from typing import List, Dict

from app.core.config import settings


def load_conversion_config(config_path: str) -> Dict:
    with open(config_path, 'r') as config_file:
        return json.load(config_file)


def convert_spec_to_soda_cl(spec_path: str) -> str:
    with open(spec_path, 'r') as spec_file:
        spec_data = json.load(spec_file)

    config_path = settings.config_path
    conversion_config = load_conversion_config(config_path)

    soda_cl_content = convert_to_soda_cl(spec_data, conversion_config)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    soda_check_path = os.path.join(settings.soda_spec_dir, settings.soda_check_template.format(timestamp=timestamp))

    print("converted soda cl content:", soda_cl_content)

    with open(soda_check_path, 'w') as soda_file:
        soda_file.write(soda_cl_content)

    return soda_check_path


def map_json_type_to_soda_type(json_type: str) -> str:
    type_mappings = {
        "text": "string",
        "decimal": "decimal(10,0)",
        "integer": "integer",
        "double": "double",
        # Add other type mappings as needed
    }
    return type_mappings.get(json_type.lower(), "string")


def convert_to_soda_cl(spec_data: List[Dict], config: Dict) -> str:
    soda_cl_content = ""

    for spec in spec_data:
        for sheet in spec["details"]["sheets"]:
            table_name = sheet.get('table_name', 'default')

            soda_cl_content += f"checks for {table_name}:\n"

            for default_check in config.get("default_checks", []):
                if "check" in default_check:
                    soda_cl_content += f"  - {default_check['check']}\n"

            if "default_schema" in config.get("schema_checks", {}):
                for schema_check in config["schema_checks"]["default_schema"]:
                    soda_cl_content += "  - schema:\n"
                    if 'name' in schema_check:
                        soda_cl_content += f"      name: {schema_check['name']}\n"

                    required_columns = schema_check.get('required_columns') or [
                        col['source_col_name'] for col in sheet['columns'] if col.get("is_required") == "Y"
                    ]
                    if required_columns:
                        soda_cl_content += "      fail:\n"
                        soda_cl_content += f"        when required column missing: [{', '.join(required_columns)}]\n"

                    wrong_column_types = {
                        col['source_col_name']: map_json_type_to_soda_type(col['source_col_type'])
                        for col in sheet['columns']
                    }
                    if wrong_column_types:
                        soda_cl_content += "        when wrong column type:\n"
                        for col_name, col_type in wrong_column_types.items():
                            soda_cl_content += f"          {col_name}: {col_type}\n"

            if "optional" in config.get("schema_checks", {}):
                for schema_check in config["schema_checks"]["optional"]:
                    soda_cl_content += "  - schema:\n"
                    if 'name' in schema_check:
                        soda_cl_content += f"      name: {schema_check['name']}\n"

            soda_cl_content += "\n"

    return soda_cl_content



