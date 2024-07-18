from typing import Dict, Any
import boto3
from boto3.dynamodb.conditions import Key, Attr
from app.models.dynamodb_models import ValidationRun, ValidationResult

class DynamoDBClient:
    def __init__(self, region_name: str = 'us-east-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.validation_run_table = self.dynamodb.Table('ValidationRun')
        self.validation_result_table = self.dynamodb.Table('ValidationResult')

    def create_validation_run(self, run: ValidationRun):
        self.validation_run_table.put_item(Item=run.to_dict())

    def update_validation_run(self, run_id: str, updates: Dict[str, Any]):
        update_expression = 'SET ' + ', '.join(f'{k}=:{k}' for k in updates.keys())
        expression_attribute_values = {f':{k}': v for k, v in updates.items()}
        self.validation_run_table.update_item(
            Key={'run_id': run_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def create_validation_result(self, result: ValidationResult):
        self.validation_result_table.put_item(Item=result.to_dict())

    def fetch_spec_file(self, dynamoDbKey: str) -> str:
        response = self.validation_run_table.get_item(Key={'dynamoDbKey': dynamoDbKey})
        return response['Item']['spec_file_path'] if 'Item' in response else None

    def fetch_validation_run(self, run_id: str) -> ValidationRun:
        response = self.validation_run_table.get_item(Key={'run_id': run_id})
        return ValidationRun.from_dict(response['Item']) if 'Item' in response else None
