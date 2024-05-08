import sys
import os
import logging

sys.path.append(os.path.dirname(__file__))

from entity.source import Source
from task_handler import TaskHandler
from utils.helper import Factory, init_environment_variables, get_environment_variables
from entity.schemas import MyHandlerEnvVars

@init_environment_variables(model=MyHandlerEnvVars)
def interval_lambda_handler(event, context):
    logging.info('start process')
    source: Source = Factory[event["source"]]()
    env_vars = get_environment_variables()
    task_handler = TaskHandler(source, env_vars)
    task_handler.process_pull_and_push()
    logging.info('end process')


@init_environment_variables(model=MyHandlerEnvVars)
def regions_population_lambda_handler(event, context):
    logging.info('start process')
    env_vars = get_environment_variables()
    source: Source = Factory[event["source"]]()
    task_handler = TaskHandler(source, env_vars)
    most_population_by_region = task_handler.process_aggregate_by_regions_population()
    logging.info('end process')
    return {"region_by_pop": most_population_by_region, "source":event["source"]}


@init_environment_variables(model=MyHandlerEnvVars)
def most_language_by_region_lambda_handler(event, context):
    logging.info('start process')
    env_vars = get_environment_variables()
    source: Source = Factory[event["source"]]()
    most_population_by_region = event["region_by_pop"]
    task_handler = TaskHandler(source, env_vars)
    res = task_handler.process_get_most_language_region(most_population_by_region)
    return res
    logging.info('end process')


