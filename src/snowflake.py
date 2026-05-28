from datetime import datetime
from typing import Any, Optional

from pandas import DataFrame as DFPandas
from polars import DataFrame as DFPolars
from pydantic._internal._model_construction import ModelMetaclass

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError
from src.aws.interface import AWS, AWSAuth
from src.aws.s3 import upload_parquet_to_s3_V2
from src.credentials import format_api_name
from src.errors import SkillException
from src.secrets import SnowflakeAuth, resolve_secrets, resolve_snowflake_private_key
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
    is_hippa: bool,
    table_config_map: dict[str, TableConfig],
    use_f_num_db: bool,
    schema_override: Optional[str] = None,
    stage_override: Optional[str] = None,
    s3_partition_override: Optional[str] = None,
    db_name_override: Optional[str] = None,
    table_name_lower_override: Optional[bool] = False,
    column_name_override: Optional[bool] = False,
):
    try:
        aws_auth, snowflake_auth = await resolve_secrets(is_hippa=is_hippa)

        database = (
            db_name_override
            if db_name_override
            else resolve_db_name(is_hippa=is_hippa, use_f_num_db=use_f_num_db, f_number=f_number)
        )
        schema = schema_override if schema_override else f"{api.lower()}_{f_number}"
        stage = stage_override if stage_override else None
        s3_partition = s3_partition_override if s3_partition_override else None

        aws = AWS(aws_auth)
        pkb = resolve_snowflake_private_key(aws=aws, is_hippa=is_hippa, pk_password=snowflake_auth.password)

        with snowflake.connector.connect(
            user=snowflake_auth.user,
            password=snowflake_auth.password,
            account=snowflake_auth.account,
            warehouse=snowflake_auth.warehouse,
            database=database,
            schema=schema,
            private_key=pkb,
            session_parameters={
                "QUERY_TAG": f"{api.lower()}_{f_number}",
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
                    is_hippa=is_hippa,
                    schema_name=schema,
                    stage_name=stage,
                    s3_partition=s3_partition,
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
                    model=config.model, pydatatype_sqltype_map=PY_SNOWFLAKE_DATATYPES_MAP
                )

                if isinstance(config.df, DFPandas):
                    if config.df.empty:
                        print(f"No data to upload for {table_name}")
                        continue
                if isinstance(config.df, DFPolars):
                    if config.df.is_empty():
                        print(f"No data to upload for {table_name}")
                        continue

                file_name = upload_parquet_to_s3_V2(aws_auth=aws_auth, df=config.df, upload_config=upload_config)

                rows_inserted, rows_updated = await merge_parquet_to_snowflake_v3(
                    upload_config=upload_config,
                    snowflake_conn=conn,
                    file_name=file_name,
                    reflected_schema_dict=schema_dict,  # type: ignore
                    table_name_override=table_name_lower_override,
                    column_name_override=column_name_override,
                )
                print(f"Inserted: {rows_inserted} | Updated: {rows_updated} rows into {table_name}")

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
            print(f"Schema {upload_config.schema_name} is not provisioned - auto provisioning and retrying")
            with snowflake.connector.connect(
                user=snowflake_auth.user,
                account=snowflake_auth.account,
                warehouse=snowflake_auth.warehouse,
                database=database,
                schema=schema,
                private_key=pkb,
                session_parameters={
                    "QUERY_TAG": f"{api.lower()}_{f_number}",
                },
            ) as retry_conn:
                provision_schema(upload_config=upload_config, snowflake_conn=retry_conn)

            await upload_to_snowflake(
                api=api,
                f_number=f_number,
                is_hippa=is_hippa,
                table_config_map=table_config_map,
                use_f_num_db=use_f_num_db,
                schema_override=schema_override,
                stage_override=stage_override,
                s3_partition_override=s3_partition_override,
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
    aws_auth, snowflake_auth = await resolve_secrets(is_hippa=IS_HIPPA)

    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=IS_HIPPA, pk_password=snowflake_auth.password)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        private_key=pkb,
        database="METADATA_DB",
        schema="API_INTEGRATIONS_ACTIVE_ROWS",
        session_parameters={
            "QUERY_TAG": "internal",
        },
    ) as mar_conn:
        cursor = mar_conn.cursor()
        sql, values = construct_insert_statement_tuple(dict_or_model_instance=mar_record, table_name="API_MAR")  # type: ignore
        cursor.execute(sql, values)


async def insert_compute_usage_metrics(payload: dict):
    aws_auth, snowflake_auth = await resolve_secrets(is_hippa=False)
    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=False, pk_password=snowflake_auth.password)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        private_key=pkb,
        warehouse=snowflake_auth.warehouse,
        database="METADATA_DB",
        schema="API_INTEGRATIONS_ACTIVE_ROWS",
        session_parameters={
            "QUERY_TAG": "internal",
        },
    ) as mar_conn:
        cursor = mar_conn.cursor()
        sql, values = construct_insert_statement_tuple(dict_or_model_instance=payload, table_name="API_COMPUTE_USAGE")
        cursor.execute(sql, values)


async def insert_prefect_metrics(payload: dict):
    aws_auth, snowflake_auth = await resolve_secrets(is_hippa=False)
    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=False, pk_password=snowflake_auth.password)

    with snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        private_key=pkb,
        warehouse=snowflake_auth.warehouse,
        database="METADATA_DB",
        schema="API_INTEGRATIONS_ACTIVE_ROWS",
        session_parameters={
            "QUERY_TAG": "internal",
        },
    ) as mar_conn:
        cursor = mar_conn.cursor()
        sql, values = construct_insert_statement_tuple(dict_or_model_instance=payload, table_name="PREFECT_METRICS")
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
    file_location = f"@{upload_config.stage_name}/{upload_config.table}/{upload_config.date_partition}/{file_name}"

    if upload_config.merge_join_list is None:
        raise ValueError("Merge Join Keys List is NONE")

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
                schema_columns.append(column.lower())  # Ensure all columns are lowercase
            schema_columns_and_types_tuple.append((column, sql_type))

    using_statement = ", ".join([f'$1:"{column}" "{column.upper()}"' for column in schema_columns])
    if column_name_override:
        on_statement = "".join(
            f'target."{column}" = source."{column.upper()}" AND ' for column in upload_config.merge_join_list
        )[:-4]  # REMOVES THE LAST "AND"
    else:
        on_statement = "".join(
            f'target."{column.lower()}" = source."{column.upper()}" AND ' for column in upload_config.merge_join_list
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
        [f'CAST(source."{column.upper()}" as {type_cast})' for column, type_cast in schema_columns_and_types_tuple]
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

    if any(char in upload_config.table for char in invalid_chars) and (not table_name_override):
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
        print(f"Merging {file_location} into {upload_config.table}")
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


def provision_table(
    model: ModelMetaclass, upload_config: UploadConfig, snowflake_conn: SnowflakeConnection, table_name_override: bool
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


def provision_schema(upload_config: UploadConfig, snowflake_conn: SnowflakeConnection, is_hippa: bool = False):
    cursor = snowflake_conn.cursor()

    schema_name = upload_config.schema_name
    stage_name = upload_config.stage_name
    stage_url = upload_config.stage_url

    storage_integration = "DREMIO_STORAGE_INTEGRATION"
    if is_hippa:
        storage_integration = "HIPPA_STORAGE_INTEGRATION"

    # Create and use schema
    print(f"Creating schema {schema_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    cursor.execute(f"USE SCHEMA {schema_name}")

    # Create file format
    cursor.execute("CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT TYPE = PARQUET COMPRESSION = auto;")

    # Create stage
    print(f"Creating stage {stage_name}")
    cursor.execute(
        f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = {stage_url}
        STORAGE_INTEGRATION = {storage_integration}
        FILE_FORMAT = PARQUET_FORMAT;
        """
    )

    # Enable stage
    print(f"Enabling stage {stage_name}")
    cursor.execute(f"ALTER STAGE {stage_name} SET DIRECTORY = (ENABLE = TRUE)")
    cursor.execute(f"ALTER STAGE {stage_name} SET DIRECTORY = (AUTO_REFRESH = TRUE);")
    cursor.execute(f"ALTER STAGE {stage_name} REFRESH;")

    print(f"Granting permissions to schema {schema_name}")
    cursor.execute(f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE PUBLIC;")
    cursor.execute(f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE SYSADMIN")
    cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE PUBLIC;")
    cursor.execute(f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE PUBLIC;")
    cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE SYSADMIN;")
    cursor.execute(f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE SYSADMIN;")


def set_boolean_types(boolean_cols: list[str], columns_list: list[str], sql_type_string: str):
    """
    Snowflake perceives nullable boolean columns as NUMBER(38, 0). This function replaces those columns with the correct type.
    """
    for index, column in enumerate(columns_list):
        for col in boolean_cols:
            if col in str(column):
                columns_list[index] = columns_list[index].replace("NUMBER(38, 0)", sql_type_string)
    return columns_list


def set_datetime_types(date_cols: list[str], columns_list: list[str], sql_type_string: str):
    """
    Snowflake perceives nullable datetime columns as TEXT. This function replaces those columns with the correct type.
    """
    for index, column in enumerate(columns_list):
        for col in date_cols:
            if col in str(column):
                columns_list[index] = (
                    columns_list[index].replace("TEXT", sql_type_string).replace("NUMBER(38, 0)", sql_type_string)
                )
    return columns_list


def set_columns_types(fields_to_change: dict[Any, list[Any]], columns_list: list[str]):
    datatype_sql_map = PY_SNOWFLAKE_DATATYPES_MAP
    columns_list = set_boolean_types(
        boolean_cols=fields_to_change[bool], columns_list=columns_list, sql_type_string=datatype_sql_map[bool]
    )  # type: ignore
    columns_list = set_datetime_types(
        date_cols=fields_to_change[datetime], columns_list=columns_list, sql_type_string=datatype_sql_map[datetime]
    )  # type: ignore
    return ",\n".join(columns_list)


async def merge_parquet_to_snowflake_table_v2(
    snowflake_auth: SnowflakeAuth,
    aws_auth: AWSAuth,
    table_name: str,
    upload_config: UploadConfig,
    file_name: str,
    fields_to_enforce_schema: dict[Any, list[str]],
    merge_join_list: list[str],
):
    api_name = format_api_name(upload_config.api_name)
    f_number = upload_config.f_number
    s3_bucket = upload_config.bucket

    aws = AWS(aws_auth)
    pkb = resolve_snowflake_private_key(aws=aws, is_hippa=False, pk_password=snowflake_auth.password)

    conn = snowflake.connector.connect(
        user=snowflake_auth.user,
        password=snowflake_auth.password,
        account=snowflake_auth.account,
        warehouse=snowflake_auth.warehouse,
        database="API_SOLUTION_DATABASE",
        private_key=pkb,
    )
    cursor = conn.cursor()

    schema_name = f"{api_name}_{f_number}"
    stage_url = f"s3://{s3_bucket}/{api_name}/{f_number}"
    stage_name = f"{api_name}_{f_number}_STAGE"

    if upload_config.s3_partition:
        parquet_file_location = f"@{stage_name}/{'/'.join(upload_config.s3_partition.split('/')[2:])}/{file_name}"

    cursor.execute(f"USE SCHEMA {schema_name}")

    # Create Stage if it doesn't exist
    cursor.execute(
        f"""CREATE OR REPLACE STAGE {stage_name} URL = {stage_url} STORAGE_INTEGRATION = DREMIO_STORAGE_INTEGRATION FILE_FORMAT = PARQUET_FORMAT;"""
    )
    cursor.execute(f"ALTER STAGE {stage_name} SET DIRECTORY = (ENABLE = TRUE)")
    cursor.execute(f"ALTER STAGE {stage_name} SET DIRECTORY = (AUTO_REFRESH = TRUE);")
    cursor.execute(f"ALTER STAGE {stage_name} REFRESH;")
    cursor.execute("""CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT TYPE = PARQUET COMPRESSION = auto;""")

    # Generate Parquet Schema
    cursor.execute(
        f"""
    SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'table') AS COLUMNS
    FROM TABLE (
    INFER_SCHEMA(
        LOCATION=> '{parquet_file_location}',
        FILE_FORMAT=>'PARQUET_FORMAT'
    )
    );
    """
    )

    new_parquet_schema = cursor.fetchall()[0][0]

    if fields_to_enforce_schema:
        new_parquet_schema = set_columns_types(
            fields_to_change=fields_to_enforce_schema, columns_list=new_parquet_schema.split(",\n")
        )
    print(f"Creating table {table_name}")

    cursor.execute(f"create table if not exists {table_name} ({new_parquet_schema.lower()})")
    # Get Column Names and Data Types.
    cursor.execute(
        f"select column_name, DATA_TYPE from Information_schema.columns where Table_name = '{table_name.upper()}' and TABLE_SCHEMA = '{schema_name.upper()}'"
    )
    existing_table_schema = cursor.fetchall()
    # Columns in Current Table
    column_names, column_types = zip(*existing_table_schema)
    parquet_columns = [element for index, element in enumerate(new_parquet_schema.split('"')) if index % 2 == 1]
    # parquet_types   = [column_set.split('"')[-1] for column_set in new_parquet_schema.split(',\n')]

    if columns_to_add := set(parquet_columns) - set(
        [column.lower() for column in column_names]
    ):  # ADD NEW COLUMNS, IF PARQUET HAS ADDITIONAL
        new_column_schema = ", ".join(
            [
                column_type
                for column_type in new_parquet_schema.split(",\n")
                if column_type.split('"')[1] in columns_to_add
            ]
        )
        alter_sql = f"ALTER TABLE {table_name} add {new_column_schema}"
        cursor.execute(alter_sql)
        cursor.execute(
            f"select column_name, DATA_TYPE from Information_schema.columns where Table_name = '{table_name.upper()}' and TABLE_SCHEMA = '{schema_name.upper()}'"
        )
        existing_table_schema = cursor.fetchall()
        column_names, column_types = zip(*existing_table_schema)

    # Filter Snowflake Column Names to match parquet columns
    # Note that Snowflake Columns may have upper/lower case not matching to parquet columns
    filtered_SFColumns = [
        (column, column_type)
        for column, column_type in zip(column_names, column_types)
        if column.lower() in set(parquet_columns)
    ]
    filtered_columns, _ = zip(*filtered_SFColumns)

    # Create sub-strings for merge statement. NOTE THAT SPECIFIC QUOTATION MARKS AND UPPER/LOWERCASE SYNTAX ARE NECESSARY OR IT WILL BREAK.
    # Grab Column names of parquet file, GRABBING FROM PARQUET FILE WILL MAINTAIN CAPITALIZATION, NOT POSSIBLE FROM COLUMNSINFORMATION
    parquet_columns_string = ", ".join(
        [f'$1:"{column}" "{column.upper()}"' for column in parquet_columns]
    )  # YOU CAN'T USE COLUMN_NAMES IN PLACE OF PARQUET_COLUMNS, IT WILL INSERT NULL IF CAPITALIZATION NOT MATCHING
    merge_join_string = "".join(
        f'target."{column.lower()}" = source."{column.upper()}" AND ' for column in merge_join_list
    )[0:-4]

    # This is so fucking complicated
    matched_columnsString = ", ".join(
        f'target."{column}" = CAST(source."{column.upper()}" as {type_cast})'
        for column, type_cast in filtered_SFColumns
    )
    not_matched_targetColumns = ", ".join([f'"{column}"' for column in filtered_columns])
    not_matched_sourceColumns = ", ".join(
        [f'CAST(source."{column.upper()}" as {type_cast})' for column, type_cast in filtered_SFColumns]
    )

    # Merge to Table
    query = f"""
    merge into {table_name} as target using 
    (select {parquet_columns_string} from {parquet_file_location} ) as source
    ON {merge_join_string}
    when matched then update set {matched_columnsString}
    when not matched then insert ({not_matched_targetColumns}) values ({not_matched_sourceColumns})
    """

    try:
        cursor.execute(query)
        cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));")
        result = cursor.fetchone()
        # (293, 0) initial insert
        # (0, 293) second insert (no changes)
        if result is not None:
            rows_added, rows_updated = result
            rows_added = int(rows_added)
            rows_updated = int(rows_updated)
            if rows_added == 0 and rows_updated == 0:
                print(f"{upload_config.table} but DataFrame was not empty. Check the UploadConfig")
                raise Exception(f"{upload_config.table} but DataFrame was not empty. Check the UploadConfig")
            return rows_added, rows_updated
        else:
            return 0, 0
    except Exception as e:
        raise e


class Snowflake:
    database: str
    schema: str
    is_hippa: bool
    session: str

    def __init__(self, db_name: str, schema_name: str, is_hippa: bool = False, session: str = "F001000"):
        self.database = db_name
        self.schema = schema_name
        self.is_hippa = is_hippa
        self.session = session

        self.connection = None
        self.cursor = None
        self.snowflake_auth = None

    async def initialize(self):
        if self.snowflake_auth is None:
            aws_auth, self.snowflake_auth = await resolve_secrets(is_hippa=self.is_hippa)

        aws = AWS(aws_auth)
        self._create_connection(aws=aws)

    def _create_connection(self, aws: AWS):
        if self.connection:
            self.connection.close()

        if self.snowflake_auth is None:
            raise SkillException

        pkb = resolve_snowflake_private_key(aws=aws, is_hippa=self.is_hippa, pk_password=self.snowflake_auth.password)

        self.connection = snowflake.connector.connect(
            user=self.snowflake_auth.user,
            account=self.snowflake_auth.account,
            warehouse=self.snowflake_auth.warehouse,
            database=self.database,
            schema=self.schema,
            private_key=pkb,
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
