from datastore.dynamodb import DynamoDBManager
from entity.schemas import MyHandlerEnvVars
from entity.source import Source

class TaskHandler:
    def __init__(self, source: Source, env: MyHandlerEnvVars):
        self.source = source
        self.dynamodb = DynamoDBManager(env)

    def process_pull_and_push(self):
        self.source.pulling_data()
        items = self.source.make_to_dynamodb()
        self.dynamodb.batch_insert_items(self.source.db_name, items)

    def process_aggregate_by_regions_population(self):
        res = self.dynamodb.most_populations_by_regions(self.source.db_name, self.source.partition_key)
        return res


    def process_get_most_language_region(self, regions):
        res = []
        for k,v in regions.items():
            res.append(self.dynamodb.get_most_language_region(self.source.db_name, self.source.partition_key, k))
            return res

