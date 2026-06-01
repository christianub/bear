from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Union

from pandas import DataFrame as DFPandas
from polars import DataFrame as DFPolars

from pydantic._internal._model_construction import ModelMetaclass

PY_SNOWFLAKE_DATATYPES_MAP = {
    bool: "BOOLEAN",
    datetime: "TIMESTAMP",
    str: "VARCHAR",
    int: "NUMBER",
    float: "NUMBER(38,2)",
    Decimal: "NUMBER(38,10)",
    list: "ARRAY",
}


@dataclass
class TableConfig:
    merge_join_list: list[str]
    is_provisioned: bool
    df: Union[DFPandas, DFPolars]
    model: ModelMetaclass


@dataclass
class UploadConfig:
    """
    Configuration for uploading data to Snowflake.

    `table` is the table name in Snowflake.
    `merge_join_list` is a list of columns to merge on.
    """


    api_name: str
    f_number: str

    date_partition: Optional[str] = None
    table: Optional[str] = None
    merge_join_list: Optional[list[str]] = None
    schema_name: Optional[str] = None

    pydatatype_sqltype_map: dict[Any, str] = field(
        default_factory=lambda: {
            bool: "BOOLEAN",
            datetime: "TIMESTAMP",
            str: "VARCHAR",
            int: "NUMBER",
            float: "NUMBER(38,2)",
            Decimal: "NUMBER(38,10)",
            list: "ARRAY",
        }
    )

    def __post_init__(self):
        if self.schema_name is None:
            self.schema_name = f"{self.api_name}_{self.f_number}"

