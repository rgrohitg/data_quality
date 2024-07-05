import unittest
import json
from app.services.converter import convert_spec_to_soda_cl

class TestConverter(unittest.TestCase):
    def test_convert_spec_to_soda_cl(self):
        spec_path = "./data/specification.json"

        # Create a mock specification file for testing
        mock_spec_data = [
            {
                "spec_name": "test new",
                "version": 1,
                "spec_key": "spec_key1",
                "updated_by": "X1234567",
                "rowkey": "d2323sfasd-sadasd-fsafs/testing_new_db_v2/234923423/spec_key",
                "created_by": "X1234567",
                "status": "APPROVED",
                "is_active": "Y",
                "details": {
                    "spec_description": "test new",
                    "sheets": [
                        {
                            "job_name": "SOME JOB NAME",
                            "uniqueTuple": [
                                "Col1"
                            ],
                            "columns": [
                                {
                                    "source_col_type": "text",
                                    "dest_col_name": "col_1",
                                    "source_col_name": "Col1",
                                    "dest_col_type": "text",
                                    "is_required": "Y",
                                    "valid_regex": ".*"
                                },
                                {
                                    "source_col_type": "decimal",
                                    "dest_col_name": "col_2",
                                    "source_col_name": "Col2",
                                    "dest_col_type": "decimal",
                                    "is_required": "Y",
                                    "valid_regex": ".*"
                                }
                            ],
                            "start_row_idx": "1",
                            "header_row_idx": "0",
                            "sheet_name": "Sheet1",
                            "table_name": "some_schema.some_table_name"
                        }
                    ]
                }
            }
        ]

        with open(spec_path, 'w') as spec_file:
            json.dump(mock_spec_data, spec_file)

        soda_check_path = convert_spec_to_soda_cl(spec_path)

        with open(soda_check_path, 'r') as soda_file:
            soda_cl_content = soda_file.read()

        expected_content = """
        checks for some_schema.some_table_name:
          - missing_count(Col1) = 0
          - invalid_count(Col1) = 0: valid format { regex: '.*' }
          - duplicate_count(Col1) = 0
          - missing_count(Col2) = 0
          - invalid_count(Col2) = 0: valid format { regex: '.*' }
        """

        self.assertEqual(soda_cl_content.strip(), expected_content.strip())

if __name__ == "__main__":
    unittest.main()
