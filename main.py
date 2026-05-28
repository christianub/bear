
from prefect import flow
from instantGMP.config import FlowParams
from instantGMP.deploy import run_instant_gmp

@flow 
def run(flow_params: FlowParams | dict) -> None:
    """Run the Instant GMP ETL pipeline."""
    if not isinstance(flow_params, FlowParams):
        flow_params = FlowParams(**flow_params)
    asyncio.run(run_instant_gmp(flow_params=flow_params))

