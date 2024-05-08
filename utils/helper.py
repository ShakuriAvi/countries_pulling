from entity.countries import Countries
import os
from typing import Any, TypeVar, Dict, List
from aws_lambda_powertools.middleware_factory import lambda_handler_decorator
from pydantic import BaseModel, ValidationError
from entity.schemas import MyHandlerEnvVars

Model = TypeVar("Model", bound=BaseModel)

ENV_CONF: MyHandlerEnvVars = None


@lambda_handler_decorator
def init_environment_variables(handler, event, context, model: MyHandlerEnvVars) -> Any:
    global ENV_CONF
    try:

        # parse the os environment variables dict
        ENV_CONF = model(**os.environ)
    except (ValidationError, TypeError) as exc:
        raise ValueError(f'failed to load environment variables, exception={str(exc)}') from exc

    return handler(event, context)


def get_environment_variables() -> MyHandlerEnvVars:
    global ENV_CONF
    if ENV_CONF is None:
        raise Exception("Environment variables not set")
    return ENV_CONF


Factory = {
    'countries': Countries
}


