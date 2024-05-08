from datastore.dynamodb import DynamoDBManager
from entity.schemas import MyHandlerEnvVars
from entity.source import Source

class TaskHandler:
    def __init__(self, source: Source, env: MyHandlerEnvVars):
        self.source = source
        self.dynamodb = DynamoDBManager(env)

    def process(self):
        items_stored = self.source.pulling_data()
        items = self.source.make_to_dynamodb()
        self.dynamodb.batch_insert_items('countries_world', items)


