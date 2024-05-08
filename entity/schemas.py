from pydantic import BaseModel, constr,  StrictInt, Field

class MyHandlerEnvVars(BaseModel):
    ACCESS_KEY: constr(min_length=1)
    SECRET_KEY: constr(min_length=1)
    REGION_NAME: constr(min_length=1)


