import os
from datetime import datetime

import httpx
from pydantic import BaseModel, model_validator
from pydantic._internal._model_construction import ModelMetaclass


class Metadata(BaseModel):
    created_at: str
    invocations: int
    last_run_date: str
    total_failures: int

    @model_validator(mode="before")
    def validate(cls, values):
        created_at = values.get("created_at")
        if not created_at:
            values["created_at"] = datetime.now().strftime("%Y-%m-%d")
        return values


def format_api_name(api: str):
    return api.lower().replace("-", "_")


def provision_credentials(Model: ModelMetaclass, credentials):
    x_api_key = os.environ.get("BC_KEY")
    if not x_api_key:
        raise ValueError("BC_KEY environment variable is required")
    payload = Model(customer=credentials).model_dump()
    res = httpx.post(
        url="https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/customer/API",
        headers={"accept": "application/json", "content-type": "application/json", "x-api-key": x_api_key},
        json=payload,
    )
    return res.json()


class Credentials(BaseModel):
    api: str
    refresh_rate: str
    meta: Metadata
    flow_params: dict
    is_active: bool


class Customer(BaseModel):
    customer: Credentials


def format_credentials_payload(
    api: str,
    cron: str,
    flow_params_payload: dict,
    created_at: str,
    invocations_override: int = 0,
    last_run_date_override: str = "",
):
    credentials = Credentials(
        api=format_api_name(api),
        refresh_rate=cron,
        flow_params=flow_params_payload,
        meta=Metadata(invocations=invocations_override, last_run_date=last_run_date_override, total_failures=0, created_at=created_at),
        is_active=True,
    )
    return credentials
