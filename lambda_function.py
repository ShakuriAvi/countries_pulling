import sys
import os
import logging

sys.path.append(os.path.dirname(__file__))

from entity.source import Source
from task_handler import TaskHandler
from utils.helper import Factory, init_environment_variables, get_environment_variables
from entity.schemas import MyHandlerEnvVars

@init_environment_variables(model=MyHandlerEnvVars)
def lambda_handler(event, context):
    logging.info('start process')
    source: Source = Factory[event["source"]]()
    env_vars = get_environment_variables()
    task_handler = TaskHandler(source, env_vars)
    task_handler.process()
    source.pulling_data()
    logging.info('end process')



