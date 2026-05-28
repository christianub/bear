from json import dumps
from typing import Optional

import httpx
import requests
from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass

from src.api_utils import fetch
from src.credentials import Credentials, format_api_name


class UpdateFlowParams(BaseModel):
    api: str
    customer_number: str
    flow_params: dict


class FlowParamsAPI:
    headers: dict
    base_url = "https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/customer/API"

    def __init__(self, x_api_key: str, debug: bool = False) -> None:
        self.headers = {"accept": "application/json", "content-type": "application/json", "x-api-key": x_api_key}
        self.debug = debug

    def delete_customer_obj(self, api: str, f_number: str):
        body = dumps(dict(customer_number=f_number, api=format_api_name(api)))
        return requests.delete(url=self.base_url, headers=self.headers, data=body, timeout=10)

    def update_flow_params(self, api: str, flow_params: dict, customer_number: Optional[str] = None):
        if not customer_number:
            payload = UpdateFlowParams(api=api, customer_number=f"{flow_params['f_number']}", flow_params=flow_params)
        else:
            payload = UpdateFlowParams(api=api, customer_number=customer_number, flow_params=flow_params)

        res = httpx.patch(url=f"{self.base_url}/flowParams", headers=self.headers, json=payload.model_dump())
        return res

    def provision_credentials(self, Model: ModelMetaclass, credentials):
        payload = Model(customer=credentials).model_dump_json()

        res = httpx.post(
            url="https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/customer/API",
            headers=self.headers,
            data=payload,
        )
        return res.json()

    async def fetch_conf_obj(self, api: str, f_number: Optional[str] = None):
        api = format_api_name(api=api)
        if api:
            url = f"https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/api/customers?api={api}"
            if f_number:
                url = f"{url}&f_number={f_number}"
            data = await fetch(url=url, headers=self.headers)
            return data["message"]
        else:
            data = await fetch(
                url="https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/api/all",
                headers=self.headers,
            )
            return data

    async def get_deployment_configurations(self, cron: str, api: str, environment_tag: str) -> list[Credentials]:
        url = f"https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/api/customers/by_refresh_rate?api={api}&refresh_rate={cron}"

        if environment_tag == "prod":
            url = f"https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/api/customers/by_refresh_rate?api={api}&refresh_rate={cron}&prod=True"

        res = await fetch(url, headers=self.headers, debug=True, timeout=120)

        return [Credentials(**deployment_conf) for deployment_conf in res["message"]]

    async def change_cron(self, api: str, f_number: str, new_cron: str):
        import httpx

        url = "https://4prmxp9dv3.execute-api.us-east-1.amazonaws.com/bc/customer/API/refreshRate"
        data = {"api": api, "customer_number": f_number, "refresh_rate": new_cron}
        res = httpx.patch(url=url, data=data, timeout=120)
        return res
