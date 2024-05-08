import boto3
from boto3.dynamodb.conditions import Key, Attr

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

    def most_populations_by_regions(self, table_name, partition_key):
        items = self.scan_tables(table_name)
        pop_by_region = self.__get_sum_of_population_by_region(items, partition_key)
        most_populations = max(pop_by_region.values())
        return {k:v for k,v in pop_by_region.items() if v == most_populations}


    def scan_tables(self, table_name):
        results = []
        last_evaluated_key = None
        while True:
            if last_evaluated_key:
                response = self.dynamodb.scan(
                    TableName=table_name,
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = self.dynamodb.scan(TableName=table_name)
            last_evaluated_key = response.get('LastEvaluatedKey')

            results.extend(response['Items'])

            if not last_evaluated_key:
                break
        return results

    def __get_sum_of_population_by_region(self, response, partition_key):
        sum_populations_by_regions = dict()
        for item in response:
            key = item[partition_key]['S']
            if key not in sum_populations_by_regions:
                sum_populations_by_regions[key] = 0
            population = int(item['population']["N"])
            sum_populations_by_regions[key] += population

        return sum_populations_by_regions


    def get_most_language_region(self, table_name, partition_key, region):
        res = self.get_query(table_name,partition_key,region)
        language_by_region = self.__get_sum_of_language_region(res)
        most_language_countries = max(language_by_region.values())
        return {k:v for k,v in language_by_region.items() if v == most_language_countries}
    def __get_sum_of_language_region(self, response):
        sum_language_by_regions = dict()
        for item in response['Items']:
            key = item["languages"]['S']
            if key not in sum_language_by_regions:
                sum_language_by_regions[key] = 0
            sum_language_by_regions[key] += 1

        return sum_language_by_regions

    def get_query(self,table_name, partition_key, partition_val, index_key=None, index_val=None):
        response = self.dynamodb.query(
        TableName=table_name,
        KeyConditionExpression='#pk = :val',  # Using placeholder for partition key
        ExpressionAttributeNames={'#pk': partition_key},  # Mapping placeholder to actual attribute name
        ExpressionAttributeValues={':val': {'S': partition_val}}  # Assuming partition_key is a string attribute
    )
        return response

