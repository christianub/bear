from instantgmp.config import Auth
from src.api_utils import post


async def call_materials_query(auth: Auth):
    """Function for calling the IGMP query materials endpoint"""
    print('---BEGINNING MATERIALS QUERY---')
    isPullComplete = False
    page = 1
    materials = []
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
                        "PartNumber": "",
                        "MaterialNameContains": "",
                        "MaterialIdContains": "",
                        "TypesContains": "",
                        "MaterialType": "",
                        "MaterialHidden": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/Materials/APIMaterialV1/Query', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            materials.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING MATERIALS QUERY---')
    return materials