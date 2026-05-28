from instantgmp.config import Auth
from src.api_utils import post

async def call_requisitions_query(auth: Auth):
    """Function for calling the IGMP query requisitions endpoint"""
    print('---BEGINNING REQUISITIONS QUERY---')
    isPullComplete = False
    page = 1
    requisitions = []
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
                        "RequisitionNumber": "",
                        "PartNumber": "",
                        "POnumberContains": "",
                        "MaterialNameContains": "",
                        "MaterialIdContains": "",
                        "Status": "",
                        "VendorId": "",
                        "FromApprovalDate": "",
                        "ToApprovalDate": "",
                        "Hidden": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/Inventory/APIRequisitionV1/Query', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            requisitions.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING REQUISITIONS QUERY---')
    return requisitions