import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import snowflake.connector
from pandas import DataFrame as DFPandas
from polars import DataFrame as DFPolars
from pydantic._internal._model_construction import ModelMetaclass
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

from src.credentials import format_api_name
from src.errors import SkillException
from src.secrets import SnowflakeAuth, resolve_secrets
from src.upload_config import PY_SNOWFLAKE_DATATYPES_MAP, TableConfig, UploadConfig
from src.util import (
    MARRecord,
    clean_up,
    construct_insert_statement_tuple,
    construct_schema_dict,
    construct_table_schema,
)


def clean_column_names(df: DFPandas) -> DFPandas:
    """
    Replaces specific patterns in column names:
    - '__dot__' → '.'
    - 'underscore____' → '__'
    Returns a new DataFrame with updated column names.
    """
    new_columns = []
    for col in df.columns:
        col = col.replace("__dot__", ".")
        col = col.replace("underscore____", "__")
        new_columns.append(col)

    df.columns = new_columns
    return df


def write_local_parquet(df: DFPandas | DFPolars, upload_config: UploadConfig) -> str:
    """Write a dataframe to a local parquet file for Snowflake PUT/merge."""
    file_name = (
        f"{upload_config.table}-{datetime.now().strftime('%Y-%m-%d_%H_%M_%S')}.parquet"
    )
    if isinstance(df, DFPandas):
        df.to_parquet(file_name, engine="pyarrow", compression="gzip")
    elif isinstance(df, DFPolars):
        df.write_parquet(file_name, compression="gzip")
    else:
        raise TypeError(f"Unsupported dataframe type: {type(df)}")
    return file_name


def resolve_db_name(is_hippa: bool, use_f_num_db: bool, f_number: str):
    """
    Returns the f_number in flow_params or the name of generic db for respective environment
    """
    if use_f_num_db:
        return f_number
    if is_hippa:
        return "HIPPA_DATABASE"
    return "API_SOLUTION_DATABASE"


async def upload_to_snowflake(
    api: str,
    f_number: str,
    table_config_map: dict[str, TableConfig],
    use_f_num_db: bool,
    schema_override: Optional[str] = None,
    db_name_override: Optional[str] = None,
    table_name_lower_override: Optional[bool] = False,
    column_name_override: Optional[bool] = False,
):
    try:
        snowflake_auth = await resolve_secrets()
        try:
            database = db_name_override
        except KeyError:
            raise ValueError("Database name is required")
        schema = schema_override if schema_override else f"{api.lower()}"

        with snowflake.connector.connect(
            user=snowflake_auth.user,
            password=snowflake_auth.password,
            account=snowflake_auth.account,
            warehouse=snowflake_auth.warehouse,
            database=database,
            schema=schema,
            session_parameters={
                "QUERY_TAG": f"{api.lower()}",
            },
        ) as conn:
            today = datetime.now()
            for table_name, config in table_config_map.items():
                config.df = clean_column_names(config.df)

                upload_config = UploadConfig(
                    api_name=format_api_name(api),
                    table=table_name,
                    f_number=f_number,
                    date_partition=f"year={today.year}/month={today.month}/day={today.day}",
                    merge_join_list=config.merge_join_list,
                    schema_name=schema,
                )
                if not config.is_provisioned:
                    print(f"Provisioning table: {table_name}")
                    provision_table(
                        model=config.model,
                        upload_config=upload_config,
                        snowflake_conn=conn,
                        table_name_override=table_name_lower_override,
                    )

                schema_dict = construct_schema_dict(
                    model=config.model,
                    pydatatype_sqltype_map=PY_SNOWFLAKE_DATATYPES_MAP,
                )

                if isinstance(config.df, DFPandas):
                    if config.df.empty:
                        print(f"No data to upload for {table_name}")
                        continue
                if isinstance(config.df, DFPolars):
                    if config.df.is_empty():
                        print(f"No data to upload for {table_name}")
                        continue

                file_name = write_local_parquet(
                    df=config.df, upload_config=upload_config
                )

                rows_inserted, rows_updated = await merge_parquet_to_snowflake_v3(
                    upload_config=upload_config,
                    snowflake_conn=conn,
                    file_name=file_name,
                    reflected_schema_dict=schema_dict,  # type: ignore
                    table_name_override=table_name_lower_override,
                    column_name_override=column_name_override,
                )
                print(
                    f"Inserted: {rows_inserted} | Updated: {rows_updated} rows into {table_name}"
                )

                mar_record = MARRecord(
                    f_number=f_number,
                    api=api,
                    inserted_count=rows_inserted,
                    updated_count=rows_updated,
                    table_name=table_name,
                )
                await insert_mar_count_metadata(mar_record=mar_record)
                clean_up(file_name)

    except ProgrammingError as e:
        if e.errno in [90106, 2003]:
            print(
                f"Schema {upload_config.schema_name} is not provisioned - auto provisioning and retrying"
            )
            with snowflake.connector.connect(
                user=snowflake_auth.user,
                password=snowflake_auth.password,
                account=snowflake_auth.account,
                warehouse=snowflake_auth.warehouse,
                database=database,
                schema=schema,
                session_parameters={
                    "QUERY_TAG": f"{api.lower()}_{f_number}",
                },
            ) as retry_conn:
                provision_schema(upload_config=upload_config, snowflake_conn=retry_conn)

            await upload_to_snowflake(
                api=api,
                f_number=f_number,
                table_config_map=table_config_map,
                use_f_num_db=use_f_num_db,
                schema_override=schema_override,
                db_name_override=db_name_override,
                table_name_lower_override=table_name_lower_override,
                column_name_override=column_name_override,
            )

        else:
            raise e


async def delete_rows_by_list_of_pkeys(
    cursor: SnowflakeCursor, table: str, pkey_column_name: str, pkeys: list[str]
) -> int:
    pkeys_to_delete = ", ".join(repr(pkey) for pkey in pkeys)

    statement = f'DELETE from {table} WHERE "{pkey_column_name}" IN ({pkeys_to_delete})'
    cursor.execute(statement)

    result = cursor.fetchone()

    if result:
        return result[0]

    return 0


async def insert_mar_count_metadata(mar_record: MARRecord) -> None:
    IS_HIPPA = False
    snowflake_auth = await resolve_secrets(is_hippa=IS_HIPPA)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database="METADATA_DB",
        schema="API_INTEGRATIONS_ACTIVE_ROWS",
        session_parameters={
            "QUERY_TAG": "internal",
        },
    ) as mar_conn:
        cursor = mar_conn.cursor()
        sql, values = construct_insert_statement_tuple(
            dict_or_model_instance=mar_record, table_name="API_MAR"
        )  # type: ignore
        cursor.execute(sql, values)


async def insert_compute_usage_metrics(payload: dict):
    snowflake_auth = await resolve_secrets(is_hippa=False)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database="METADATA_DB",
        schema="API_INTEGRATIONS_ACTIVE_ROWS",
        session_parameters={
            "QUERY_TAG": "internal",
        },
    ) as mar_conn:
        cursor = mar_conn.cursor()
        sql, values = construct_insert_statement_tuple(
            dict_or_model_instance=payload, table_name="API_COMPUTE_USAGE"
        )
        cursor.execute(sql, values)


async def insert_prefect_metrics(payload: dict):
    snowflake_auth = await resolve_secrets(is_hippa=False)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database="METADATA_DB",
        schema="API_INTEGRATIONS_ACTIVE_ROWS",
        session_parameters={
            "QUERY_TAG": "internal",
        },
    ) as mar_conn:
        cursor = mar_conn.cursor()
        sql, values = construct_insert_statement_tuple(
            dict_or_model_instance=payload, table_name="PREFECT_METRICS"
        )
        cursor.execute(sql, values)


async def merge_parquet_to_snowflake_v3(
    upload_config: UploadConfig,
    snowflake_conn: SnowflakeConnection,
    file_name: str,
    reflected_schema_dict: dict[Any, str],
    table_name_override: bool,
    column_name_override: bool,
) -> tuple[int, int]:
    cursor = snowflake_conn.cursor()

    if upload_config.merge_join_list is None:
        raise ValueError("Merge Join Keys List is NONE")

    local_path = Path(file_name).resolve()
    if not local_path.is_file():
        raise FileNotFoundError(f"Local parquet file not found: {local_path}")

    internal_stage = f"{upload_config.schema_name}_INTERNAL_STAGE"
    cursor.execute(
        "CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT TYPE = PARQUET COMPRESSION = AUTO;"
    )
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {internal_stage}")

    put_path = local_path.as_posix().replace("'", "''")
    cursor.execute(
        f"PUT 'file://{put_path}' @{internal_stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )
    staged_file = os.path.basename(file_name)
    file_location = f"@{internal_stage}/{staged_file} (FILE_FORMAT => 'PARQUET_FORMAT')"

    schema_columns = []
    schema_columns_and_types_tuple = []
    for py_type, columns in reflected_schema_dict.items():
        sql_type = upload_config.pydatatype_sqltype_map.get(
            py_type, "VARCHAR"
        )  # Use default VARCHAR if type is not found
        for column in columns:
            if column_name_override:
                schema_columns.append(column)
            else:
                schema_columns.append(
                    column.lower()
                )  # Ensure all columns are lowercase
            schema_columns_and_types_tuple.append((column, sql_type))

    using_statement = ", ".join(
        [f'$1:"{column}" "{column.upper()}"' for column in schema_columns]
    )
    if column_name_override:
        on_statement = "".join(
            f'target."{column}" = source."{column.upper()}" AND '
            for column in upload_config.merge_join_list
        )[:-4]  # REMOVES THE LAST "AND"
    else:
        on_statement = "".join(
            f'target."{column.lower()}" = source."{column.upper()}" AND '
            for column in upload_config.merge_join_list
        )[:-4]  # REMOVES THE LAST "AND"

    casted_update_set = ", ".join(
        f'target."{column}" = CAST(source."{column.upper()}" as {type_cast})'
        for column, type_cast in schema_columns_and_types_tuple
    )
    if column_name_override:
        target_columns = ", ".join([f'"{column}"' for column in schema_columns])
    else:
        target_columns = ", ".join([f'"{column.lower()}"' for column in schema_columns])
    source_columns = ", ".join(
        [
            f'CAST(source."{column.upper()}" as {type_cast})'
            for column, type_cast in schema_columns_and_types_tuple
        ]
    )
    invalid_chars = [
        " ",
        "&",
        "%",
        "$",
        "#",
        "@",
        "!",
        "*",
        "(",
        ")",
        "-",
        "+",
        "=",
        ",",
        "/",
        "\\",
        "?",
        ".",
        ":",
        ";",
        "[",
        "]",
        "{",
        "}",
        "<",
        ">",
        "|",
        '"',
        "'",
    ]

    if any(char in upload_config.table for char in invalid_chars) and (
        not table_name_override
    ):
        merge_statement = f"""
        MERGE INTO \"{upload_config.table.upper()}\" AS target 
        USING (select {using_statement} from {file_location} ) as source
        ON {on_statement}
        when matched then
            update set
                {casted_update_set}
        when not matched then
            insert ({target_columns})
            values ({source_columns})
            """
    else:
        merge_statement = f"""
        MERGE INTO {upload_config.table} AS target 
        USING (select {using_statement} from {file_location} ) as source
        ON {on_statement}
        when matched then
            update set
                {casted_update_set}
        when not matched then
            insert ({target_columns})
            values ({source_columns})
            """

    # Custom override
    if table_name_override:
        merge_statement = f"""
        MERGE INTO \"{upload_config.table}\" AS target 
        USING (select {using_statement} from {file_location} ) as source
        ON {on_statement}
        when matched then
            update set
                {casted_update_set}
        when not matched then
            insert ({target_columns})
            values ({source_columns})
            """
    try:
        print(
            f"Merging {staged_file} from local path {local_path} into {upload_config.table}"
        )
        cursor.execute(merge_statement)
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
        result = cursor.fetchone()
        # (293, 0) initial insert
        # (0, 293) second insert (no changes)
        if result is not None:
            rows_added, rows_updated = result
            rows_added = int(rows_added)
            rows_updated = int(rows_updated)
            if rows_added == 0 and rows_updated == 0:
                print(
                    f"WARNING: No rows inserted or updated for {upload_config.table}. "
                    "Source data may already be up-to-date."
                )
                return 0, 0
            return rows_added, rows_updated
        else:
            return 0, 0

    except Exception as e:
        raise e
    finally:
        cursor.execute(f"REMOVE @{internal_stage}/{staged_file}")


def provision_table(
    model: ModelMetaclass,
    upload_config: UploadConfig,
    snowflake_conn: SnowflakeConnection,
    table_name_override: bool,
):
    cursor = snowflake_conn.cursor()

    schema_dict = construct_schema_dict(model=model)

    if upload_config.table:
        create_schema_statement = construct_table_schema(
            table_name=upload_config.table,
            schema_dict=schema_dict,
            pydatatype_sqltype_map=upload_config.pydatatype_sqltype_map,
            override=table_name_override,
        )
    print(f"Creating table {upload_config.table} in schema {upload_config.schema_name}")
    print(f"With schema: {create_schema_statement}")
    cursor.execute(create_schema_statement)


def provision_schema(
    upload_config: UploadConfig,
    snowflake_conn: SnowflakeConnection,
    is_hippa: bool = False,
):
    cursor = snowflake_conn.cursor()

    schema_name = upload_config.schema_name
    internal_stage = f"{schema_name}_INTERNAL_STAGE"

    print(f"Creating schema {schema_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    cursor.execute(f"USE SCHEMA {schema_name}")

    cursor.execute(
        "CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT TYPE = PARQUET COMPRESSION = AUTO;"
    )

    print(f"Creating internal stage {internal_stage}")
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {internal_stage}")

    print(f"Granting permissions to schema {schema_name}")
    cursor.execute(f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE PUBLIC;")
    cursor.execute(f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE SYSADMIN")
    cursor.execute(
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE PUBLIC;"
    )
    cursor.execute(
        f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE PUBLIC;"
    )
    cursor.execute(
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE SYSADMIN;"
    )
    cursor.execute(
        f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE SYSADMIN;"
    )


class Snowflake:
    database: str
    schema: str
    is_hippa: bool
    session: str

    def __init__(
        self,
        db_name: str,
        schema_name: str,
        is_hippa: bool = False,
        session: str = "F001000",
    ):
        self.database = db_name
        self.schema = schema_name
        self.is_hippa = is_hippa
        self.session = session

        self.connection = None
        self.cursor = None
        self.snowflake_auth = None

    async def initialize(self):
        if self.snowflake_auth is None:
            self.snowflake_auth = await resolve_secrets(is_hippa=self.is_hippa)

        self._create_connection()

    def _create_connection(self):
        if self.connection:
            self.connection.close()

        if self.snowflake_auth is None:
            raise SkillException

        self.connection = snowflake.connector.connect(
            user=self.snowflake_auth.user,
            password=self.snowflake_auth.password,
            account=self.snowflake_auth.account,
            warehouse=self.snowflake_auth.warehouse,
            database=self.database,
            schema=self.schema,
            session_parameters={"QUERY_TAG": self.session},
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query: str, params=None):
        if self.cursor is None:
            raise SkillException
        return self.cursor.execute(query, params)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.connection = None
        self.cursor = None
