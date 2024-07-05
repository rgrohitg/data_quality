import unittest
from app.services.validator import validate_data

class TestValidator(unittest.TestCase):
    def test_validate_data(self):
        data_file_path = "./data/data.csv"
        soda_check_path = "./data/soda_check.yml"  # Assuming this file exists for testing

        results = validate_data(data_file_path, soda_check_path)

        self.assertIn("summary", results)
        self.assertIn("checks", results)
        self.assertEqual(results["summary"], "Validation completed successfully.")

if __name__ == "__main__":
    unittest.main()
