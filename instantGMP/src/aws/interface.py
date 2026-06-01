import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from io import BytesIO, StringIO
from time import sleep
from typing import Any, Optional, Union
import boto3
from botocore.exceptions import ClientError
from pandas import DataFrame, read_csv, read_parquet
from pydantic import BaseModel, model_validator
from pydantic._internal._model_construction import ModelMetaclass # type: ignore


def apply_partition_trailing_slash(obj: dict):
    last_char = obj["prefix"][-1]
    if last_char != "/":
        obj["prefix"] = obj["prefix"] + "/"
    return obj


class S3ContinuationConfig(BaseModel):
    access_key: str
    secret_key: str
    region: str
    bucket: str
    prefix: str
    continuation_token: Optional[str]

    @model_validator(mode="before")
    def alter_prefix(cls, obj):
        obj = apply_partition_trailing_slash(obj)
        return obj


@dataclass
class AWSThrottleConf:
    s3_ls_batch_size: int = 100
    job_batch_size: int = 10
    max_threads: int = 4


@dataclass
class AWSClients:
    ATHENA = "athena"
    S3 = "s3"
    SM = "secretsmanager"


class AWSAuth(BaseModel):
    access_key: str
    secret_key: str
    region: str = "us-east-1"


class AWS:
    def __init__(self, aws_auth: AWSAuth, debug: bool = False):
        self.session = None
        self.clients = AWSClients()
        self.aws_auth = aws_auth
        self.debug = debug

    def _init_session(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_auth.access_key,
            aws_secret_access_key=self.aws_auth.secret_key,
            region_name=self.aws_auth.region,
        )
        return session

    def _init_client(self, client_name: str):
        session = self._init_session()
        return session.client(client_name)

    def _init_athena_client(self):
        return self._init_client(client_name=self.clients.ATHENA)

    def _init_sm_client(self):
        return self._init_client(client_name=self.clients.SM)

    def _init_s3_client(self):
        return self._init_client(client_name=self.clients.S3)

    def athena_exec_query_to_df(self, database: str, output_location: str, query: str, wait_interval: int = 5) -> DataFrame:
        athena_client = self._init_athena_client()

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )

        query_execution_id = response["QueryExecutionId"]

        while True:
            status_response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            sleep(wait_interval)

        if status != "SUCCEEDED":
            raise Exception(f"Query {status}. Reason: {status_response['QueryExecution']['Status']['StateChangeReason']}")

        result_paginator = athena_client.get_paginator("get_query_results")
        result_iterator = result_paginator.paginate(QueryExecutionId=query_execution_id)

        columns = []
        rows = []
        for page in result_iterator:
            if not columns:
                columns = [col["Label"] for col in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
            for row in page["ResultSet"]["Rows"][1:]:
                rows.append([datum.get("VarCharValue", None) for datum in row["Data"]])
        df = DataFrame(rows, columns=columns)  # type: ignore
        return df

    def sm_get_secret(self, secret_id: str, full_secret: bool = False) -> Optional[Union[str, dict]]:
        sm = self._init_sm_client()
        try:
            if self.debug:
                print(f"Fetching secret: {secret_id}")
            secret = sm.get_secret_value(SecretId=secret_id)
            if full_secret:
                return secret
            return json.loads(secret["SecretString"])
        # if the secret doesn't exist stay calm, just return None
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return None
            raise e

    def sm_create_secret(self, secret_name: str, secret_values_dict: dict[Any, Any]):
        sm = self._init_sm_client()
        if self.debug:
            print(f"creating secret: {secret_name}")
        response = sm.create_secret(Name=secret_name, SecretString=json.dumps(secret_values_dict))
        return response

    def sm_update_secret(self, secret_id: str, secret_values_dict: dict[Any, Any]):
        sm = self._init_sm_client()
        if self.debug:
            print(f"updating secret: {secret_id}")
        response = sm.update_secret(SecretId=secret_id, SecretString=json.dumps(secret_values_dict))
        return response

    def s3_list_objects_v2(self, bucket: str, path: str):
        s3 = self._init_s3_client()
        objs = s3.list_objects_v2(Bucket=bucket, Prefix=path)
        return objs

    def s3_lsobjs_continuously(self, bucket: str, path: str, max_keys: int, continuation_token: Optional[str] = None):
        s3 = self._init_s3_client()
        if continuation_token:
            objs = s3.list_objects_v2(Bucket=bucket, Prefix=path, ContinuationToken=continuation_token, MaxKeys=max_keys)
        else:
            objs = s3.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=max_keys)
        if self.debug:
            print(len(objs["Contents"]))
        return objs

    def list_objects_all(self, bucket_name, prefix=None):
        s3 = self._init_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        operation_parameters = {"Bucket": bucket_name}
        if prefix:
            operation_parameters["Prefix"] = prefix

        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def _s3_get_obj_dataframe(self, bucket: str, key: str) -> DataFrame:
        s3 = self._init_s3_client()
        obj = s3.get_object(Bucket=bucket, Key=key)
        contents = obj["Body"].read().decode("utf-8")
        csv_buffer = StringIO(contents)
        df = read_csv(csv_buffer)
        return df

    def s3_get_obj_dataframe_from_parquet(self, bucket: str, key: str) -> DataFrame:
        s3 = self._init_s3_client()
        if self.debug:
            print(f"fetching parquet file: {key}")
        obj = s3.get_object(Bucket=bucket, Key=key)
        contents = BytesIO(obj["Body"].read())
        df = read_parquet(contents)
        return df

    def _collect_dataframe_batch(self, bucket_name: str, batch_keys: list, max_threads: int) -> dict[str, DataFrame]:
        results = {}
        with ThreadPoolExecutor(max_threads) as executor:
            future_to_key = {executor.submit(self._s3_get_obj_dataframe, bucket_name, key): key for key in batch_keys}
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                print("Processing file: ", key)
                try:
                    results[key] = future.result()
                except Exception as e:
                    raise e
        return results

    # def download_batch_objs(self, bucket_name: str)
    def pydantify_dataframes(self, bucket_name: str, keys: list, batch_size: int, Model: ModelMetaclass, throttle_config: AWSThrottleConf):
        results = []
        for i in range(0, len(keys), batch_size):
            batch_keys = keys[i : i + batch_size]
            batch_results: dict[str, DataFrame] = self._collect_dataframe_batch(
                bucket_name=bucket_name, batch_keys=batch_keys, max_threads=throttle_config.max_threads
            )
            for key, df in batch_results.items():
                if df.empty:
                    continue
                for _, row in df.iterrows():
                    try:
                        model_instance = Model(**row.to_dict())
                        results.append(model_instance.model_dump())
                    except Exception: 
                        print('Skipping file. Model is None.')
        return results
