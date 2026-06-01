from instantgmp.config import Auth
from src.api_utils import post


async def call_inventory_query(auth: Auth, show_depleted_inventory: bool = False):
    """Function for calling the IGMP query inventory endpoint"""
    print("---BEGINNING INVENTORY QUERY---")
    isPullComplete = False
    page = 1
    inventory = []
    while not isPullComplete:
        body = {
            "QueryIn": {
                "Connection": {
                    "UserCode": f"{auth.api_user}",
                    "UserPassword": f"{auth.api_pass}",
                },
                "Data": {
                    "Page": page,
                    "Filters": {
                        "PartNumber": "",
                        "ReceiptNumber": "",
                        "SplitFrom": "",
                        "RequisitionNumber": "",
                        "MaterialNameContains": "",
                        "MaterialIdContains": "",
                        "PurchaseOrderNumberContains": "",
                        "VendorLotNumberContains": "",
                        "InternalLotNumberContains": "",
                        "FromApprovalDate": "",
                        "ToApprovalDate": "",
                        "Project": "",
                        "StatusId": "",
                        "MaterialSystemType": "",
                        "ShowDepletedInventory": show_depleted_inventory,
                    },
                },
            }
        }
        response = await post(
            url=f"{auth.base_url}API/Inventory/APIInventoryV1/Query",
            headers={},
            json=body,
            verify=False,
            timeout=300,
        )
        if response["Succeed"] is True:
            inventory.extend(response["Response"])
            if response["IsLastPage"] is True:
                isPullComplete = True
        page += 1
    print("---FINISHING INVENTORY QUERY---")
    return inventory


async def call_inventory_usage(auth: Auth, receipt_number: str):
    """Function for calling the IGMP query inventory endpoint"""
    print(f"---BEGINNING INVENTORY USAGE QUERY FOR {receipt_number}---")
    isPullComplete = False
    page = 1
    usages = []
    while not isPullComplete:
        body = {
            "UsageIn": {
                "Connection": {
                    "UserCode": f"{auth.api_user}",
                    "UserPassword": f"{auth.api_pass}",
                },
                "Data": {
                    "Page": page,
                    "Filters": {"InventoryReceiptNumber": receipt_number},
                },
            }
        }
        response = await post(
            url=f"{auth.base_url}API/Inventory/APIInventoryV1/Usage",
            headers={},
            json=body,
            verify=False,
            timeout=300,
        )
        if response.get("Succeed", None) is True and response.get("Response", None) is not None:
            raw_response = response["Response"]
            for item in raw_response:
                item["InventoryReceiptNumber"] = receipt_number
            usages.extend(raw_response)
            if response["IsLastPage"] is True:
                isPullComplete = True
        elif response.get("Succeed", None) is True and response.get("Response", None) is None:
            if response.get("IsLastPage", None) is True:
                isPullComplete = True
            isPullComplete = True
        page += 1
    print(f"---FINISHING INVENTORY USAGE QUERY FOR {receipt_number}---")
    return usages
