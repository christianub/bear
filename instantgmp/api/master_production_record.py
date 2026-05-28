from instantgmp.config import Auth
from src.api_utils import post

async def call_master_production_record_query(auth: Auth):
    """Function for calling the IGMP query master production record endpoint"""
    print('---BEGINNING MASTER PRODUCTION RECORD QUERY---')
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
                            "FormulationIdContains": "",
                            "TheoreticalBatchYield": "",
                            "PartNumber": "",
                            "MaterialTypeNameContains": "",
                            "StatusId": "",
                            "UsersPersonId": "",
                            "Hidden": "", 
                            "AuthorPersonId": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/Batchrecord/APIMasterProductionRecordV1/Query', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            materials.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING MASTER PRODUCTION RECORD QUERY---')
    return materials
