import boto3
from botocore.exceptions import ClientError
from entity.schemas import MyHandlerEnvVars
import json

class DynamoDBManager:
    def __init__(self, dynamo_settings:MyHandlerEnvVars):
        self.dynamodb = boto3.client(
        'dynamodb',
        aws_access_key_id=dynamo_settings.ACCESS_KEY,
        aws_secret_access_key=dynamo_settings.SECRET_KEY,
        region_name=dynamo_settings.REGION_NAME
    )

    def get_table(self, table_name):
        try:
            table = self.dynamodb.Table(table_name)
            return table
        except Exception as e:
            print(f"Error getting table '{table_name}': {str(e)}")
            return None


    def insert_item(self, table_name, item):
        try:
            table = self.get_table(table_name)
            response = table.put_item(Item=item)
            print(f"Item inserted into '{table_name}' successfully.")
            return response
        except Exception as e:
            print(f"Error inserting item into '{table_name}': {str(e)}")
            return None

    def batch_insert_items(self, table_name, items):
        try:
            request_items = []
            for item in items:
                item = {k: v for k, v in item.items()}
                request_items.append({
                    'PutRequest': {
                        'Item': item
                    }
                })

            # Splitting request items into chunks of 25 (maximum batch size)
            for chunk in [request_items[i:i + 25] for i in range(0, len(request_items), 25)]:
                response = self.dynamodb.batch_write_item(
                    RequestItems={
                        table_name: chunk
                    }
                )
                if response.get('UnprocessedItems'):
                    unprocessed_items = response['UnprocessedItems'][table_name]
                    print(f"Failed to process {len(unprocessed_items)} items, retrying...")
                    retry_count = 0
                    while unprocessed_items and retry_count < 3:
                        response = self.dynamodb.batch_write_item(
                            RequestItems={
                                table_name: unprocessed_items
                            }
                        )
                        if response.get('UnprocessedItems'):
                            unprocessed_items = response['UnprocessedItems'][table_name]
                            print(f"Failed to process {len(unprocessed_items)} items, retrying...")
                            retry_count += 1
                        else:
                            print("Retry successful.")
                            break

            print(f"Batch inserted {len(items)} items into '{table_name}' successfully.")
            return True
        except ClientError as e:
            print(f"Error inserting batch items into '{table_name}': {str(e)}")
            return False
