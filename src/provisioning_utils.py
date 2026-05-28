from typing import Any, Type
import snowflake.connector
from src.secrets import resolve_secrets, resolve_snowflake_private_key
from src.snowflake import insert_mar_count_metadata, provision_schema, resolve_db_name
from src.upload_config import PY_SNOWFLAKE_DATATYPES_MAP, UploadConfig
from src.util import MARRecord, construct_schema_dict, construct_table_schema
from src.aws.interface import AWS


async def seed_api_mar(tables: list[str], api: str, is_hippa: bool, use_f_num_db: bool, f_number: str):
    aws_auth, snowflake_auth = await resolve_secrets(is_hippa=is_hippa)

    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=is_hippa, pk_password=snowflake_auth.password)
    database = resolve_db_name(is_hippa=is_hippa, use_f_num_db=use_f_num_db, f_number=f_number)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database=database,
        schema=api,
        session_parameters={
            "QUERY_TAG": f"{api.lower()}_{f_number}",
        },
        private_key=pkb,
    ) as conn:
        cursor = conn.cursor()

        for table_name in tables:
            statement = f"SELECT COUNT(*) FROM {table_name.lower()};"

            cursor.execute(statement)
            result = cursor.fetchone()

            if result is None:
                raise Exception("Could not get rows from table")
            count = result[0]
            print(f'"{table_name}": {count}')

            mar_record = MARRecord(f_number=f_number, api=api, inserted_count=count, updated_count=0, table_name=table_name)
            await insert_mar_count_metadata(mar_record=mar_record)


def construct_create_table_statement_from_pydantic_model(Model: Type[Any], table_name: str, override: bool):
    schema_dict = construct_schema_dict(model=Model, pydatatype_sqltype_map=PY_SNOWFLAKE_DATATYPES_MAP)
    create_table_statement = construct_table_schema(table_name=table_name, schema_dict=schema_dict, pydatatype_sqltype_map=PY_SNOWFLAKE_DATATYPES_MAP, override=override)

    return create_table_statement


async def provision_fnumber_schema(f_number: str, api_name: str, is_hippa: bool):
    aws_auth, snowflake_auth = await resolve_secrets(is_hippa=is_hippa)

    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=is_hippa, pk_password=snowflake_auth.password)

    upload_config = UploadConfig(api_name=api_name, f_number=f_number, is_hippa=is_hippa)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database=f_number,
        private_key=pkb,
    ) as conn:
        provision_schema(upload_config=upload_config, snowflake_conn=conn, is_hippa=is_hippa)


async def provision_nonf_hippa_schema(api_name: str, f_number: str, is_hippa: bool):
    aws_auth, snowflake_auth = await resolve_secrets(is_hippa=is_hippa)

    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=is_hippa, pk_password=snowflake_auth.password)

    upload_config = UploadConfig(api_name=api_name, f_number=f_number, is_hippa=is_hippa)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database="HIPPA_DATABASE",
        private_key=pkb
    ) as conn:
        provision_schema(upload_config=upload_config, snowflake_conn=conn, is_hippa=is_hippa)
