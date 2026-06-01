from instantgmp.deploy import run_instant_gmp
from instantgmp.config import FlowParams
from prefect import flow
from prefect.blocks.system import Secret
import asyncio

@flow
def run():
    flow_params = Secret.load("instantgmpsecrets")
    flow_params = Secret.load("prod-snowflake-role")
    flow_params = flow_params.get()
    flow_params = FlowParams(**flow_params)
    asyncio.run(run_instant_gmp(flow_params=flow_params))