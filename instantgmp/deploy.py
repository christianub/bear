import asyncio
from typing import List

from dotenv import load_dotenv
from pandas import DataFrame


from instantgmp.api.batch_production_record import (
    call_batch_production_record_query,
)
from instantgmp.api.inventory import (
    call_inventory_query,
    call_inventory_usage,
)
from instantgmp.api.master_production_record import (
    call_master_production_record_query,
)
from instantgmp.api.materials import call_materials_query
from instantgmp.api.materials_planned import call_materials_planned_query
from instantgmp.api.pending_receipts import call_pending_receipts_query
from instantgmp.api.requisitions import call_requisitions_query
from instantgmp.api.specifications import call_specifications_query
from instantgmp.config import API, FlowParams
from instantgmp.models.models import (
    BatchProductionRecord,
    InventoryQuery,
    InventoryUsage,
    MasterProductionRecord,
    MaterialPlannedQuery,
    MaterialsQuery,
    PendingReceiptsQuery,
    RequisitionsQuery,
    SpecificationsQuery,
)
from src.snowflake import upload_to_snowflake
from src.upload_config import TableConfig

load_dotenv(override=True)


async def run_instant_gmp(flow_params: FlowParams):
    """Main function to pull data from API"""
    # Calling Query API endpoints to extract data
    tasks = []
    call_materials_query_task = asyncio.create_task(
        call_materials_query(auth=flow_params.auth)
    )
    call_specifications_query_task = asyncio.create_task(
        call_specifications_query(auth=flow_params.auth)
    )
    call_materials_planned_query_task = asyncio.create_task(
        call_materials_planned_query(auth=flow_params.auth)
    )
    call_requisitions_query_task = asyncio.create_task(
        call_requisitions_query(auth=flow_params.auth)
    )
    call_pending_receipts_query_task = asyncio.create_task(
        call_pending_receipts_query(auth=flow_params.auth)
    )
    call_inventory_query_task = asyncio.create_task(
        call_inventory_query(auth=flow_params.auth)
    )
    call_inventory_query_task_depleted = asyncio.create_task(
        call_inventory_query(auth=flow_params.auth, show_depleted_inventory=True)
    )
    call_batch_production_query_task = asyncio.create_task(
        call_batch_production_record_query(auth=flow_params.auth)
    )
    call_master_production_query_task = asyncio.create_task(
        call_master_production_record_query(auth=flow_params.auth)
    )
    tasks.extend(
        [
            call_materials_query_task,
            call_specifications_query_task,
            call_materials_planned_query_task,
            call_requisitions_query_task,
            call_pending_receipts_query_task,
            call_batch_production_query_task,
            call_master_production_query_task,
            call_inventory_query_task,
            call_inventory_query_task_depleted,
        ]
    )
    query_results = await asyncio.gather(*tasks)

    # Calling Inventory Usage API endpoint to extract Data (in batches of 100)
    inventory_receipt_numbers: List[str] = [
        item["InventoryReceiptNumber"] for item in query_results[7]
    ]
    batch_size = 100
    usage_results = []
    for i in range(0, len(inventory_receipt_numbers), batch_size):
        batch = inventory_receipt_numbers[i : i + batch_size]
        inventory_usage_tasks = []
        for num in batch:
            call_inventory_usage_task = asyncio.create_task(
                call_inventory_usage(auth=flow_params.auth, receipt_number=num)
            )
            inventory_usage_tasks.append(call_inventory_usage_task)
        raw_results = await asyncio.gather(*inventory_usage_tasks)
        for result in raw_results:
            usage_results.extend(result)

    # Validating data with Pydantic models
    materials_query = [MaterialsQuery(**item).model_dump() for item in query_results[0]]
    specifications_query = [
        SpecificationsQuery(**item).model_dump() for item in query_results[1]
    ]
    materials_planned_query = [
        MaterialPlannedQuery(**item).model_dump() for item in query_results[2]
    ]
    requisitions_query = [
        RequisitionsQuery(**item).model_dump() for item in query_results[3]
    ]
    pending_receipts_query = [
        PendingReceiptsQuery(**item).model_dump() for item in query_results[4]
    ]
    batch_production_record_query = [
        BatchProductionRecord(**item).model_dump() for item in query_results[5]
    ]
    master_production_record_query = [
        MasterProductionRecord(**item).model_dump() for item in query_results[6]
    ]

    inventory_query = [
        InventoryQuery(**item).model_dump()
        for item in query_results[7] + query_results[8]
    ]

    inventory_usage = [InventoryUsage(**item).model_dump() for item in usage_results]

    # Deduplicate the batch production records
    batch_production_record_df = DataFrame(batch_production_record_query)
    if batch_production_record_df.duplicated().sum() > 0:
        batch_production_record_df = batch_production_record_df.drop_duplicates(
            ignore_index=True
        )
        print("Batch Production Records are deduplicated")

    # Loading data to snowflake
    upload_map = {
        "materials_query": TableConfig(
            df=DataFrame(materials_query),
            model=MaterialsQuery,
            merge_join_list=["part_number"],
            is_provisioned=False,
        ),
        "specifications_query": TableConfig(
            df=DataFrame(specifications_query),
            model=SpecificationsQuery,
            merge_join_list=["part_number", "version"],
            is_provisioned=False,
        ),
        "materials_planned_query": TableConfig(
            df=DataFrame(materials_planned_query),
            model=MaterialPlannedQuery,
            merge_join_list=["material_planned_id"],
            is_provisioned=False,
        ),
        "requisitions_query": TableConfig(
            df=DataFrame(requisitions_query),
            model=RequisitionsQuery,
            merge_join_list=["requisition_number", "part_number", "version"],
            is_provisioned=False,
        ),
        "pending_receipts_query": TableConfig(
            df=DataFrame(pending_receipts_query),
            model=PendingReceiptsQuery,
            merge_join_list=["receipt_number"],
            is_provisioned=False,
        ),
        "inventory_query": TableConfig(
            df=DataFrame(inventory_query),
            model=InventoryQuery,
            merge_join_list=["inventory_receipt_number"],
            is_provisioned=False,
        ),
        "inventory_usage": TableConfig(
            df=DataFrame(inventory_usage),
            model=InventoryUsage,
            merge_join_list=["inventory_receipt_number", "usage_sequence"],
            is_provisioned=False,
        ),
        "batch_production_record": TableConfig(
            df=batch_production_record_df,
            model=BatchProductionRecord,
            merge_join_list=["bpr_id", "mpr_number", "mpr_version_number"],
            is_provisioned=False,
        ),
        "master_production_record": TableConfig(
            df=DataFrame(master_production_record_query),
            model=MasterProductionRecord,
            merge_join_list=["mpr_number", "mpr_version_number"],
            is_provisioned=False,
        ),
    }
    await upload_to_snowflake(
        f_number=flow_params.f_number,
        table_config_map=upload_map,
        api=API,
        is_hippa=False,
        use_f_num_db=True,
    )

