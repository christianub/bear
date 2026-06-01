import boto3
import xmltodict

from src.api_utils import async_retry
from src.secrets import AWSAuth


async def s3_path_exist(aws_auth: AWSAuth, bucket_name: str, path_prefix: str) -> bool:
    """
    This function checks if a path exists in a s3 bucket.

    >>> should_not_run_historical = aws.s3_path_exist(bucket_name="api-ingestion-bucket", path_prefix=f"<api_name>/F000123/<example_target..>")
    >>> if should_not_run_historical: print("Not running historical data")

    Args:
        bucket_name (str): Name of the s3 bucket
        path_prefix (str): The path to check for

    Returns:
        bool: True if the path does not exist, False if it does exist.
    """
    # aws_auth = await get_aws_creds()
    s3 = boto3.client("s3", aws_access_key_id=aws_auth.access_key, aws_secret_access_key=aws_auth.secret_key)
    objs = s3.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)
    print(f"Checking if {bucket_name}/{path_prefix} exists")

    if "Contents" in objs:
        return True
    else:
        return False


async def move_s3_file(aws_auth: AWSAuth, bucket: str, source_prefix: str, target_prefix: str, files: list) -> None:
    """
    This function is meant to move files to different directories within an s3 bucket.
    """
    s3 = boto3.client("s3", aws_access_key_id=aws_auth["access_key"], aws_secret_access_key=aws_auth["secret_key"])
    for file_name in files:
        if "/" in file_name:
            file_name = file_name.split("/")[-1]
        print(f"Moving {file_name} from {source_prefix} to {target_prefix}")
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": f"{source_prefix}/{file_name}"},
            Key=f"{target_prefix}/{file_name}",
        )
        s3.delete_object(Bucket=bucket, Key=f"{source_prefix}/{file_name}")


@async_retry(max_retries=10, retry_delay=5)
async def decode_xml_obj(s3, bucket: str, key: str, file_name: str) -> tuple[str, dict]:
    """
    This function decodes files from s3 using byte strings and returns the file name and a dictionary representation of the xml file.
    """

    obj = await s3.get_object(Bucket=bucket, Key=key)
    stream = obj["Body"]
    data = b""
    async for chunk, _ in stream.iter_chunks():
        data += chunk
    decoded_data = data.decode("utf-8")
    try:
        xml_dict = xmltodict.parse(decoded_data, process_namespaces=False)
    except Exception:
        print(f"Error parsing file: {file_name}")
        return (key, None)
    return (key, xml_dict)


@async_retry(max_retries=10, retry_delay=5)
async def pull_from_s3(aws_auth: AWSAuth, config: dict):
    import aioboto3

    """
    This function pulls files from the desired s3 bucket and returns a dictionary
    of processed files and a list of names of broken files.

    config must be a dataclass like the one below: 

    @dataclass
    class CustomerConfig:
        f_number: str
        bucket: str
        path_prefix: str
        
    """

    session = aioboto3.Session()
    loaded_files = {}
    broken_files = []
    count = 1

    async with session.client(
        "s3", aws_access_key_id=aws_auth["access_key"], aws_secret_access_key=aws_auth["secret_key"]
    ) as s3:
        paginator = s3.get_paginator("list_objects_v2")
        print("Beginning file extraction...")
        async for page in paginator.paginate(Bucket=config.bucket, Prefix=f"{config.path_prefix}/"):
            for obj in page["Contents"]:
                key = obj["Key"]
                file_name = key.split("/")[-1]
                print(f"Got file: {file_name}")
                print(f"Count: {count}")
                count += 1

                if not file_name:
                    continue

                file_key, file_dict = await decode_xml_obj(s3=s3, bucket=config.bucket, key=key, file_name=file_name)

                if file_dict is None:
                    broken_files.append(file_key)
                elif len(file_dict) > 0:
                    loaded_files[file_key] = file_dict
    if len(loaded_files) == 0:
        print(f"No files to process for bucket: {config.bucket} and path: {config.path_prefix}")
        return
    return loaded_files, broken_files
