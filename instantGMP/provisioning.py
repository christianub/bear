"""Provision Instant GMP credentials and Snowflake schema (moved from sauron)."""

import asyncio
import json
import os
from dataclasses import dataclass
from os import getenv
from typing import Any, Type

from dotenv import load_dotenv

from instantgmp.config import API, FlowParams
from src.credentials import Customer, format_credentials_payload
from src.flow_params_api import FlowParamsAPI
from src.provisioning_utils import (
    construct_create_table_statement_from_pydantic_model,
    provision_fnumber_schema,
    seed_api_mar,
)

load_dotenv(override=True)


@dataclass
class Databases:
    API_SOLUTION_DATABASE = "API_SOLUTION_DATABASE"
    HIPPA_DATABASE = "HIPPA_DATABASE"
    METADATA_DB = "METADATA_DB"


class Provisioning:
    databases = Databases()

    def construct_create_table_statement(
        self, Model: Type[Any], table_name: str, table_name_lower_override: bool
    ):
        return construct_create_table_statement_from_pydantic_model(
            Model=Model, table_name=table_name, override=table_name_lower_override
        )

    def provision_fnum_schema(self, f_number: str, api_name: str, is_hippa: bool):
        asyncio.run(provision_fnumber_schema(f_number=f_number, api_name=api_name, is_hippa=is_hippa))

    def seed_mar(self, tables: list[str], api: str, f_number: str, is_hippa: bool, use_f_num_db: bool):
        asyncio.run(
            seed_api_mar(
                tables=tables, api=api, f_number=f_number, is_hippa=is_hippa, use_f_num_db=use_f_num_db
            )
        )


if __name__ == "__main__":
    x_api_key = getenv("BC_KEY")
    if x_api_key is None:
        raise ValueError("BC_KEY required")

    fp_api = FlowParamsAPI(x_api_key=x_api_key)
    f_number = "F001226"
    cron = "0 5 * * *"
    is_hippa = False

    flow_params = json.loads(os.environ["INSTANT_GMP_CONFIG"])
    fp = FlowParams(**flow_params)

    credentials = format_credentials_payload(
        api=API,
        cron=cron,
        flow_params_payload=fp.model_dump(),
        created_at="04/10/2025",
    )

    res = fp_api.provision_credentials(Model=Customer, credentials=credentials)
    print(f"create response: {res}")

    provision = Provisioning()
    provision.provision_fnum_schema(f_number=f_number, api_name=API, is_hippa=is_hippa)
    provision.seed_mar(
        tables=["table1", "table2"], api=API, is_hippa=False, use_f_num_db=True, f_number=f_number
    )
    print("finished")
