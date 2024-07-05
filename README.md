# Data Quality Service

This is a FastAPI application that validates data against a given specification using SodaCL.

## Installation

To install the required dependencies, run:

```bash
pip install fastapi uvicorn pydantic soda-cli

```

## Running the Application
### To run the application, execute the following command in your terminal:
uvicorn app.main:app --reload

## API Endpoints
### The application has one API endpoint:

#### POST /validate: Validates data against a given specification.

#### Example request body:

```json
{
    "spec_key": "/path/to/your/specification.json",
    "data_file": "/path/to/your/data.csv"
}
```


#### Example response:

```json
{
    "status": "success",
    "results": {
        "summary": "Validation completed successfully.",
        "checks": [
            {
                "name": "column_exists",
                "status": "PASS",
                "failed_rows": []
            },
            {
                "name": "value_range",
                "status": "FAIL",
                "failed_rows": [
                    {"row_number": 2, "value": "out_of_range_value"}
                ]
            },
            ...
        ]
    }
}

```
