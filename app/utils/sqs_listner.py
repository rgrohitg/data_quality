# app/utils/sqs_listener.py
import boto3
import json
from app.services.file_processor import file_processor

class SQSListener:
    def __init__(self):
        self.sqs = boto3.client('sqs', region_name='your-region')

    def listen_to_sqs(self, queue_url):
        while True:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            messages = response.get('Messages', [])
            if messages:
                for message in messages:
                    self.process_message(message)
                    self.sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

    def process_message(self, message):
        body = json.loads(message['Body'])
        try:
            validation_result = file_processor.process_file(body['dynamoDbKey'], body['s3TempKey'])
            print(validation_result)
        except Exception as e:
            print(f"Failed to process message: {e}")

sqs_listener = SQSListener()
