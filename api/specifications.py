from instantgmp.config import Auth
from src.api_utils import post

async def call_specifications_query(auth: Auth):
    """Function for calling the IGMP query specifications endpoint"""
    print('---BEGINNING SPECIFICATIONS QUERY---')
    isPullComplete = False
    page = 1
    specifications = []
    while not isPullComplete:
        body = {
            "QuerySpecificationsIn": {
                "Connection": {
                "UserCode": f"{auth.api_user}",
                "UserPassword": f"{auth.api_pass}"
                },
                "Data": {
                    "Page": page,
                    "Filters": {
                        "MaterialNameContains": "",
                        "PartNumber": "",
                        "TypesContains": "",
                        "StatusFilter": "",
                        "MaterialType": "",
                        "Hidden": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/Specifications/APISpecificationsV1/QuerySpecifications', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            specifications.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING SPECIFICATIONS QUERY---')
    return specifications
