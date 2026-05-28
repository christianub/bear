import json
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from pydantic import BaseModel

from src.api_utils import async_retry
from src.aws.interface import AWS, AWSAuth
from src.errors import SkillException


class SnowflakeAuth(BaseModel):
    account: str
    user: str
    password: str
    warehouse: str


def _load_json_secret(secret_id: str) -> dict:
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        raise ValueError(f"Failed to load secret {secret_id!r}") from e
    return json.loads(response["SecretString"])


async def get_secrets(is_hippa: bool) -> tuple[AWSAuth, SnowflakeAuth]:
    if is_hippa:
        snowflake_block = "hipa-snowflake-role"
        aws_block = "hippa-aws-secret"
    else:
        snowflake_block = "prod-snowflake-role"
        aws_block = "api-aws-creds"

    snowflake_secrets = _load_json_secret(snowflake_block)
    aws_secrets = _load_json_secret(aws_block)

    return AWSAuth(**aws_secrets), SnowflakeAuth(**snowflake_secrets)


async def get_secret(secret: str) -> dict:
    return _load_json_secret(secret)


@async_retry(max_retries=5, retry_delay=3)
async def resolve_secrets(is_hippa: bool = False) -> tuple[AWSAuth, SnowflakeAuth]:
    return await get_secrets(is_hippa=is_hippa)


def load_p8_key(p8_file_path: str, password: str | bytes):
    if isinstance(password, str):
        password = password.encode()

    with open(p8_file_path, "r") as key_file:
        private_key = key_file.read()

    return private_key


async def provision_snowflake_private_rsa_key_from_p8_file(
    user: str, sm_key_name: str, p8_file_path: str, password: str, aws: AWS
):
    key = load_p8_key(p8_file_path=p8_file_path, password=password)
    secret_values_dict = {
        "user": user,
        "password": password,
        "private_key": key,
        "last_rotate": datetime.now(timezone.utc).isoformat(),
    }
    res = aws.sm_update_secret(secret_id=sm_key_name, secret_values_dict=secret_values_dict)
    return res


def resolve_snowflake_private_key(aws: AWS, is_hippa: bool, pk_password: str):
    sm_env_key_name = ""
    if is_hippa:
        sm_env_key_name = "hipa_snowflake_private_key"
    else:
        sm_env_key_name = "prod_snowflake_private_key"
    secret = aws.sm_get_secret(secret_id=sm_env_key_name, full_secret=False)

    if secret is None or isinstance(secret, str):
        raise SkillException

    if not isinstance(secret["private_key"], str):
        raise SkillException

    p_key = load_pem_private_key(
        secret["private_key"].encode(), password=pk_password.encode(), backend=default_backend()
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pkb
