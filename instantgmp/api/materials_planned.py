from src.api_utils import post
from instantgmp.config import Auth


async def call_materials_planned_query(auth: Auth):
    """Function for calling the IGMP query material planned endpoint"""
    print('---BEGINNING MATERIALS PLANNED QUERY---')
    isPullComplete = False
    page = 1
    materials_planned = []
    while not isPullComplete:
        body = {
            "QueryIn": {
                "Connection": {
                "UserCode": f"{auth.api_user}",
                "UserPassword": f"{auth.api_pass}"
                },
                "Data": {
                    "Page": page,
                    "Filters": {
                        "FromOrderBy": "",
                        "ToOrderBy": "",
                        "FromNeedBy": "",
                        "ToNeedBy": "",
                        "PartNumber": "",
                        "MaterialNameContains": "",
                        "VendorId": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/Inventory/APIMaterialPlannedV1/Query', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            materials_planned.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING MATERIALS PLANNED QUERY---')
    return materials_planned