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
    Configuration for uploading data to S3 & Snowflake.

    `f_number` is used to provision the schema, which can be done without data.

    `date_partition` should always be defined. \n
    `table` is the table name in Snowflake. \n
    `merge_join_list` is a list of columns to merge on.
    """

    api_name: str
    f_number: str
    is_hippa: bool

    bucket: str = "api-ingestion-bucket"
    date_partition: Optional[str] = None
    table: Optional[str] = None
    merge_join_list: Optional[list[str]] = None

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

    s3_partition: Optional[str] = None
    schema_name: Optional[str] = None
    stage_url: Optional[str] = None
    stage_name: Optional[str] = None

    def __post_init__(self):
        if self.is_hippa:
            self.bucket = "hippa-compliance-bucket"

        if self.stage_name is None:  # SproutVideo, Data_For_SEO
            self.stage_name = f"{self.api_name.lower()}_{self.f_number}_stage"

        if self.s3_partition is not None:  # SproutVideo
            self.s3_partition = f"{self.s3_partition}"
            
        elif self.s3_partition is None:
            self.s3_partition = f"{self.api_name.lower().replace('_', '')}/{self.f_number}"

        if self.schema_name is None:
            self.schema_name = f"{self.api_name}_{self.f_number}"

        self.stage_url = f"s3://{self.bucket}/{self.s3_partition}" 
        self.s3_partition = f"{self.s3_partition}/{self.table}/{self.date_partition}"

