from instantgmp.config import Auth
from src.api_utils import post


async def call_batch_production_record_query(auth: Auth):
    """Function for calling the IGMP query batch production record endpoint"""
    print('---BEGINNING BATCH PRODUCTION RECORD QUERY---')
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
                            "ProjectTitle": "",
                            "ProductName": "",
                            "MPRNumber": "",
                            "MPRVersionNumber": "",
                            "BPRNumber": "",
                            "PartNumber": "",
                            "ProductNumber": "",
                            "To": "",
                            "From": "",
                            "StatusId": "",
                            "UsersPersonId": "",
                            "Hidden": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/BatchRecord/APIBatchProductionRecordV1/Query', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            materials.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING BATCH PRODUCTION RECORD QUERY---')
    return materials
