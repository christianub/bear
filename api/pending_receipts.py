from instantgmp.config import Auth
from src.api_utils import post

async def call_pending_receipts_query(auth: Auth):
    """Function for calling the IGMP query pending receipts endpoint"""
    print('---BEGINNING PENDING RECEIPTS QUERY---')
    isPullComplete = False
    page = 1
    pending_receipts = []
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
                        "RequisitionNumber": "",
                        "MaterialNameContains": "",
                        "PurchaseOrderNumberContains": "",
                        "Project": "",
                        "ShowReceipt": ""
                    }
                }
            }
        }
        response = await post(url=f'{auth.base_url}API/Inventory/APIPendingReceiptV1/Query', headers={}, json=body, verify=False, timeout=300)
        if response['Succeed'] is True:
            pending_receipts.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print('---FINISHING PENDING RECEIPTS QUERY---')
    return pending_receipts