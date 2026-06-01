import json
import warnings
from datetime import datetime, timezone

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from prefect.blocks.system import Secret
from pydantic import BaseModel

from src.api_utils import async_retry
from src.aws.interface import AWS
from src.errors import SkillException

warnings.filterwarnings("ignore", category=RuntimeWarning)


class SnowflakeAuth(BaseModel):
    account: str
    user: str
    password: str
    warehouse: str


async def _load_prefect_json_block(block_name: str) -> dict:
    try:
        secret_block = Secret.load(block_name)
        raw = secret_block.get()
    except AttributeError as e:
        if "'coroutine' object has no attribute 'get'" in str(e):
            secret_block = await Secret.load(block_name)  # type: ignore
            raw = secret_block.get()
        else:
            raise

    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    raise ValueError(f"Unexpected secret format for Prefect block {block_name!r}")


async def get_snowflake_auth() -> SnowflakeAuth:
    block_name = "prod-snowflake-role"
    data = await _load_prefect_json_block(block_name)
    return SnowflakeAuth(**data)


async def get_secret(block_name: str) -> dict:
    return await _load_prefect_json_block(block_name)


@async_retry(max_retries=5, retry_delay=3)
async def resolve_secrets() -> SnowflakeAuth:
    return await get_snowflake_auth()


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
