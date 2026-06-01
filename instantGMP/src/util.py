import inspect
import subprocess
import sys
import types
import uuid
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from functools import wraps
from json import dump
from os import path, remove
from pathlib import Path
from typing import Any, Type, Union

from pandas import DataFrame
from pydantic import BaseModel, Field
from pydantic._internal._model_construction import ModelMetaclass  # type: ignore
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Attachment,
    Disposition,
    FileContent,
    FileName,
    FileType,
    Mail,
)

from src.upload_config import PY_SNOWFLAKE_DATATYPES_MAP


def pydantify_dataframe(Model: Type[Any], df: DataFrame) -> DataFrame:
    items_list = []
    for _, row in df.iterrows():
        data = row.to_dict()
        usage_cost = Model(**data)
        items_list.append(usage_cost.model_dump())
    return DataFrame(items_list)


def write_json_file(file_name: str, data):
    with open(file_name, "w") as f:
        dump(data, f, indent=4)


def install_runtime_dependency(package: str):
    print(f"Installing {package}...")
    # Prefer uv while targeting the active interpreter explicitly.
    try:
        subprocess.check_call(["uv", "pip", "install", "--python", sys.executable, package])  # noqa: S603
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback to pip; bootstrap pip first when running in minimal venvs.
        try:
            subprocess.check_call([sys.executable, "-m", "ensurepip", "--upgrade"])  # noqa: S603
        except subprocess.CalledProcessError:
            pass
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])  # noqa: S603


def construct_schema_dict(
    model: Type[Any],
    pydatatype_sqltype_map: dict[Any, str] = PY_SNOWFLAKE_DATATYPES_MAP,
) -> dict[Any, list[str]]:
    """
    This function constructs a dictionary that maps every Python datatype to a list of atrributes that need to be coerced provided by the upload config.

    The datatypes provided in the UploadConfig have KNOWN issues with snowflake schema inference, therefore we enforce.

    >>> class Example(BaseModel):
    >>>     mock_column: str
    >>>     is_active: bool
    >>>     has_permission: Optional[bool]
    >>>     has_options: Optional[Union[bool, int]]
    >>>     today: datetime
    >>>     tomorrow: Union[datetime, str]
    >>>     infinity: Optional[Union[datetime, str]]

    >>> construct_schema_dict(model=Example, upload_config=upload_config)

    >>> {bool: ['is_active', 'has_permission', 'has_options'], datetime: ['today', 'tomorrow', 'infinity']}

    Args:
        model (T): Any Pydantic BaseModel or dataclass object.
        upload_config (UploadConfig): Upload configuration for the pydatatype_sqltype_map attribute.

    Returns:
        dict[Any, list[str]]: A dictionary that maps every Python datatype to a list of atrributes that need to be coerced.
    """

    datatypes = pydatatype_sqltype_map.keys()

    column_type_map = {data_type: [] for data_type in datatypes}

    # Reflect on Pydantic BaseModel
    if isinstance(model, ModelMetaclass):
        for data_type in datatypes:
            for attr_name, field in model.model_fields.items():
                attr_type = field.annotation
                attr_name = attr_name.replace("__dot__", ".")
                attr_name = attr_name.replace("underscore____", "__")
                UnionType = getattr(types, "UnionType", None)
                if attr_type == data_type or (
                    isinstance(attr_type, type) and issubclass(attr_type, data_type)
                ):
                    column_type_map[data_type].append(attr_name)
                elif (
                    hasattr(attr_type, "__origin__") and attr_type.__origin__ is Union
                ) or (UnionType is not None and type(attr_type) is UnionType):
                    union_args = attr_type.__args__
                    if data_type == union_args[0] or (
                        isinstance(union_args[0], type)
                        and issubclass(union_args[0], data_type)
                    ):
                        column_type_map[data_type].append(attr_name)

        # map at int equals set difference of map at bool
        column_type_map[int] = list(
            set(column_type_map[int]) - set(column_type_map[bool])
        )
        return column_type_map

    elif is_dataclass(model):
        for data_type in datatypes:
            for field in fields(model):
                field_type = field.type
                if field_type == data_type or (
                    isinstance(field_type, type) and issubclass(field_type, data_type)
                ):
                    column_type_map[data_type].append(field.name)
                elif (
                    hasattr(field_type, "__origin__") and field_type.__origin__ is Union
                ):  # type: ignore
                    union_args = field_type.__args__  # type: ignore
                    if data_type in union_args or any(
                        isinstance(arg, type) and issubclass(arg, data_type)
                        for arg in union_args
                    ):
                        column_type_map[data_type].append(field.name)

        column_type_map[int] = list(
            set(column_type_map[int]) - set(column_type_map[bool])
        )
        column_type_map[str] = list(
            set(column_type_map[str]) - set(column_type_map[datetime])
        )

        return column_type_map
    else:
        raise ValueError(
            "Received something that was not a Pydantic Model or Dataclass"
        )


def construct_table_schema(
    table_name: str,
    schema_dict: dict,
    pydatatype_sqltype_map: dict,
    override: bool,
    should_replace_table: bool = False,
) -> str:
    create_statement_prefix = "CREATE TABLE IF NOT EXISTS"

    if should_replace_table:
        create_statement_prefix = "CREATE OR REPLACE TABLE"

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

    if any(char in table_name for char in invalid_chars):
        sql_statement = f'{create_statement_prefix} "{table_name.upper()}" ('
    else:
        sql_statement = f"{create_statement_prefix} {table_name} ("

    # Custom override
    if override:
        sql_statement = f'{create_statement_prefix} "{table_name}" ('

    for py_type, columns in schema_dict.items():
        sql_type = pydatatype_sqltype_map.get(
            py_type, "VARCHAR"
        )  # Use default VARCHAR if type is not found
        for column in columns:
            sql_statement += (
                f' "{column}" {sql_type},'  # Put quotes to preserve case sensitivity
            )

    sql_statement = (
        sql_statement.rstrip(",\n") + ");"
    )  # Remove the last comma and add closing parenthesis

    return sql_statement


def clean_up(file_name: str) -> None:
    if path.exists(file_name):
        remove(file_name)


def clean_duplicates_with_list_cols(df: DataFrame) -> DataFrame:
    list_columns = []
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list, dict)).any():  # type: ignore
            list_columns.append(col)
            df.loc[:, col] = df[col].astype(str)
    cols_keep = [i for i in df.columns if i not in ["mapped_fields"]]
    df = df.drop_duplicates(subset=cols_keep, ignore_index=True)

    for col in list_columns:
        df.loc[:, col] = df[col].apply(eval)

    return df


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def generate_uuid():
    return str(uuid.uuid4())


class MARRecord(BaseModel):
    f_number: str
    api: str
    inserted_at: Union[datetime, str] = Field(default_factory=utc_now_iso)
    inserted_count: int
    updated_count: int
    table_name: str
    id: str = Field(default_factory=generate_uuid)


def construct_insert_statement_tuple(
    dict_or_model_instance: Union[ModelMetaclass, dict], table_name: str
) -> tuple[str, tuple]:
    if isinstance(dict_or_model_instance, dict):
        data = dict_or_model_instance
    elif "model_dump" in dir(dict_or_model_instance):
        data = dict_or_model_instance.model_dump()  # type: ignore
    else:
        raise ValueError("Function accepts a Pydantic Model or dictionary.")

    columns = ", ".join(
        f'"{col.lower()}"' for col in data.keys()
    )  # Surround each column name in double quotes and convert to lowercase

    placeholders = ", ".join(["%s"] * len(data))
    values = tuple(data.values())

    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return sql, values


def install_runtine_deps_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        caller_file = inspect.getfile(func)
        caller_file_path = Path(caller_file)
        caller_dir = caller_file_path.parent
        requirements = caller_dir / "requirements.txt"
        if requirements.exists():
            with requirements.open() as f:
                reqs = f.readlines()
                for req in reqs:
                    install_runtime_dependency(package=req.strip())

        return func(*args, **kwargs)

    return wrapper


async def send_email(
    template_id: str | None,
    message_data: dict | None | str,
    to_email: str,
    sendgrid_api_key: str | None,
    attachments: list[dict] = None,
):
    from_email = "no-reply@bearcognition.com"
    message = Mail(from_email=from_email, to_emails=to_email)
    message.template_id = template_id
    message.dynamic_template_data = message_data

    if attachments:
        for attachment in attachments:
            message_attachment = Attachment(
                FileContent(attachment["content"]),
                FileName(attachment["filename"]),
                FileType(attachment["type"]),
                Disposition(attachment["disposition"]),
            )
            message.add_attachment(message_attachment)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        sg.send(message)
    except Exception as e:
        print(e)
