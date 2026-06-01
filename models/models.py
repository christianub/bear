from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, Field, model_validator

class MaterialsQuery(BaseModel):
    part_number: str = Field(..., alias="PartNumber")
    material_name: Optional[str] = Field(None, alias="MaterialName")
    material_type: Optional[str] = Field(None, alias="Type")
    unit: Optional[str] = Field(None, alias="Unit")
    material_id: Optional[str] = Field(None, alias="MaterialId")
    description: Optional[str] = Field(None, alias="Description")
    cost: Optional[str] = Field(None, alias="Cost")
    account: Optional[str] = Field(None, alias="Account")
    storage_condition: Optional[str] = Field(None, alias="StorageCondition")
    recomended_shelf_life: Optional[str] = Field(None, alias="RecomendedShelfLife")
    retest_period: Optional[str] = Field(None, alias="RetestPeriod")
    expiry_period: Optional[str] = Field(None, alias="ExpiryPeriod")
    alert_level_quantity: Optional[str] = Field(None, alias="AlertLevelQuantity")
    reorder_level_quantity: Optional[str] = Field(None, alias="ReorderLevelQuantity")
    order_lead_time: Optional[int] = Field(None, alias="OrderLeadTime")
    material_hidden: Optional[int] = Field(None, alias="MaterialHidden")
    material_master_data: Optional[int] = Field(None, alias="MaterialMasterData")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
        return values
            

class SpecificationsQuery(BaseModel):
    part_number: str = Field(..., alias="PartNumber")
    material_name: Optional[str] = Field(None, alias="MaterialName")
    version: str = Field(..., alias="Version")
    unit: Optional[str] = Field(None, alias="Unit")
    specification_type: Optional[str] = Field(None, alias="Type")
    effective_date: Optional[Union[datetime, str]] = Field(None, alias="EffectiveDate")
    hidden: Optional[int] = Field(None, alias="Hidden")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values


class MaterialPlannedQuery(BaseModel):
    material_planned_id: str = Field(..., alias="MaterialPlannedId")
    part_number: Optional[str] = Field(None, alias="PartNumber")
    version: Optional[str] = Field(None, alias="Version")
    material_name: Optional[str] = Field(None, alias="MaterialName")
    unit: Optional[str] = Field(None, alias="Unit")
    quantity_needed: Optional[str] = Field(None, alias="QuantityNeeded")
    order_by: Optional[Union[datetime, str]] = Field(None, alias="OrderBy")
    need_by: Optional[Union[datetime, str]] = Field(None, alias="NeedBy")
    vendor_id: Optional[str] = Field(None, alias="VendorId")
    vendor_name: Optional[str] = Field(None, alias="VendorName")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values

class RequisitionsQuery(BaseModel):
    requisition_number: str = Field(..., alias="RequisitionNumber")
    status_id: Optional[int] = Field(None, alias="StatusId")
    status_name: Optional[str] = Field(None, alias="StatusName")
    part_number: str = Field(..., alias="PartNumber")
    version: str = Field(..., alias="Version")
    po_number: Optional[str] = Field(None, alias="POnumber")
    material_name: Optional[str] = Field(None, alias="MaterialName")
    material_id: Optional[str] = Field(None, alias="MaterialId")
    vendor_id: Optional[str] = Field(None, alias="VendorId")
    vendor_name: Optional[str] = Field(None, alias="VendorName")
    item_quantity: Optional[str] = Field(None, alias="ItemQuantity")
    item_quantity_unit: Optional[str] = Field(None, alias="ItemQuantityUnit")
    requested_by_person_id: Optional[str] = Field(None, alias="RequestedByPersonId")
    requested_by_person_description: Optional[str] = Field(None, alias="RequestedByPersonDescription")
    requested_by_date_time: Optional[Union[datetime, str]] = Field(None, alias="RequestedByDateTime")
    order_by: Optional[Union[datetime, str]] = Field(None, alias="OrderBy")
    needed_by: Optional[Union[datetime, str]] = Field(None, alias="NeededBy")
    approved_by_person_id: Optional[str] = Field(None, alias="ApprovedByPersonId")
    approved_by_person_description: Optional[str] = Field(None, alias="ApprovedByPersonDescription")
    approved_by_date_time: Optional[Union[datetime, str]] = Field(None, alias="ApprovedByDateTime")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values

class PendingReceiptsQuery(BaseModel):
    receipt_number: str = Field(..., alias="ReceiptNumber")
    part_number: Optional[str] = Field(None, alias="PartNumber")
    version: Optional[str] = Field(None, alias="Version")
    material_name: Optional[str] = Field(None, alias="MaterialName")
    material_id: Optional[str] = Field(None, alias="MaterialId")
    vendor_name: Optional[str] = Field(None, alias="VendorName")
    project: Optional[str] = Field(None, alias="Project")
    requisition_number: Optional[str] = Field(None, alias="RequisitionNumber")
    purchase_order_number: Optional[str] = Field(None, alias="PurchaseOrderNumber")
    quantity_ord: Optional[str] = Field(None, alias="QuantityOrd")
    quantity_rcv: Optional[str] = Field(None, alias="QuantityRcv")
    unit: Optional[str] = Field(None, alias="Unit")
    needed_by: Optional[Union[datetime, str]] = Field(None, alias="NeededBy")
    fulfillment_date_2nd: Optional[Union[datetime, str]] = Field(None, alias="FulfillmentDate2Nd")
    fulfillment_date_3rd: Optional[Union[datetime, str]] = Field(None, alias="FulfillmentDate3Rd")
    fulfillment_date_4th: Optional[Union[datetime, str]] = Field(None, alias="FulfillmentDate4Th")
    vendor_part_number: Optional[str] = Field(None, alias="VendorPartNumber")
    catalog_number: Optional[str] = Field(None, alias="CatalogNumber")
    status: Optional[str] = Field(None, alias="Status")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values


class InventoryQuery(BaseModel):
    inventory_receipt_number: str = Field(..., alias="InventoryReceiptNumber")
    part_number: Optional[str] = Field(None, alias="PartNumber")
    version: Optional[str] = Field(None, alias="Version")
    receipt_number: Optional[str] = Field(None, alias="ReceiptNumber")
    split_from: Optional[str] = Field(None, alias="SplitFrom")
    requisition_number: Optional[str] = Field(None, alias="RequisitionNumber")
    purchase_order_number: Optional[str] = Field(None, alias="PurchaseOrderNumber")
    material_name: Optional[str] = Field(None, alias="MaterialName")
    material_id: Optional[str] = Field(None, alias="MaterialId")
    vendor_name: Optional[str] = Field(None, alias="VendorName")
    project: Optional[str] = Field(None, alias="Project")
    vendor_lot_number: Optional[str] = Field(None, alias="VendorLotNumber")
    internal_lot_number: Optional[str] = Field(None, alias="InternalLotNumber")
    batch_number: Optional[str] = Field(None, alias="BatchNumber")
    production_number: Optional[str] = Field(None, alias="ProductionNumber")
    quantity_received: Optional[str] = Field(None, alias="QuantityReceived")
    quantity_received_unit: Optional[str] = Field(None, alias="QuantityReceivedUnit")
    quantity_remain: Optional[str] = Field(None, alias="QuantityRemain")
    quantity_remain_unit: Optional[str] = Field(None, alias="QuantityRemainUnit")
    received_date: Optional[Union[datetime, str]] = Field(None, alias="ReceivedDate")
    inventory_status_id: Optional[str] = Field(None, alias="InventoryStatusId")
    inventory_status_name: Optional[str] = Field(None, alias="InventoryStatusName")
    use_by: Optional[Union[datetime, str]] = Field(None, alias="UseBy")
    retest_date: Optional[Union[datetime, str]] = Field(None, alias="RetestDate")
    expiry_date: Optional[Union[datetime, str]] = Field(None, alias="ExpiryDate")
    bin_location: Optional[str] = Field(None, alias="BinLocation")
    product_name: Optional[str] = Field(None, alias="ProductName")
    vendor_part_number: Optional[str] = Field(None, alias="VendorPartNumber")
    catalog_number: Optional[str] = Field(None, alias="CatalogNumber")
    cost: Optional[str] = Field(None, alias="Cost")
    material_type: Optional[str] = Field(None, alias="MaterialType")
    full_retest_date: Optional[Union[datetime, str]] = Field(None, alias="FullRetestDate")
    micro_retest_date: Optional[Union[datetime, str]] = Field(None, alias="MicroRetestDate")
    impurity_retest_date: Optional[Union[datetime, str]] = Field(None, alias="ImpurityRetestDate")
    mfg_retest_date: Optional[Union[datetime, str]] = Field(None, alias="MFGRetestDate")
    mfg_expiry_date: Optional[Union[datetime, str]] = Field(None, alias="MFGExpiryDate")
    internal_expiry_date: Optional[Union[datetime, str]] = Field(None, alias="InternalExpiryDate")
    facility_id: Optional[str] = Field(None, alias="FacilityId")
    room_id: Optional[str] = Field(None, alias="RoomId")
    bin_id: Optional[str] = Field(None, alias="BinId")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values
    

class InventoryUsage(BaseModel):
    inventory_receipt_number: str = Field(..., alias="InventoryReceiptNumber")
    usage_sequence: int = Field(..., alias="UsageSequence")
    used_date: Optional[Union[datetime, str]] = Field(None, alias="UsedDate")
    used_production_number: Optional[str] = Field(None, alias="UsedProductionNumber")
    used_batch_number: Optional[str] = Field(None, alias="UsedBatchNumber")
    used_quantity: Optional[str] = Field(None, alias="UsedQuantity")
    used_quantity_unit: Optional[str] = Field(None, alias="UsedQuantityUnit")
    purpose: Optional[str] = Field(None, alias="Purpose")
    quantity_remain: Optional[str] = Field(None, alias="QuantityRemain")
    quantity_remain_unit: Optional[str] = Field(None, alias="QuantityRemainUnit")
    approved_by_person_id: Optional[str] = Field(None, alias="ApprovedByPersonId")
    approved_by_person_description: Optional[str] = Field(None, alias="ApprovedByPersonDescription")
    approved_date_time: Optional[Union[datetime, str]] = Field(None, alias="ApprovedDateTime")
    bin_location: Optional[str] = Field(None, alias="BinLocation")
    action: Optional[str] = Field(None, alias="Action")
    split_to_receipt_number: Optional[str] = Field(None, alias="SplitToReceiptNumber")
    new_status: Optional[str] = Field(None, alias="NewStatus")
    project: Optional[str] = Field(None, alias="Project")
    picklist_name: Optional[str] = Field(None, alias="PicklistName")
    shipping_manifest_name: Optional[str] = Field(None, alias="ShippingManifestName")
    adjustment_reason: Optional[str] = Field(None, alias="AdjustmentReason")
    vendor_part_number: Optional[str] = Field(None, alias="VendorPartNumber")
    catalog_number: Optional[str] = Field(None, alias="CatalogNumber")
    new_cost: Optional[str] = Field(None, alias="NewCost")
    old_vendor_lot_number: Optional[str] = Field(None, alias="OldVendorLotNumber")
    facility_id: Optional[str] = Field(None, alias="FacilityId")
    room_id: Optional[str] = Field(None, alias="RoomId")
    bin_id: Optional[str] = Field(None, alias="BinId")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values


class BatchProductionRecord(BaseModel):
    bpr_id: int = Field(..., alias="BprId")
    project_title: Optional[str] = Field(default=None, alias="ProjectTitle")
    mpr_version_id: Optional[str] = Field(default=None, alias="MPRVersionId")
    status_id: Optional[int] = Field(default=None, alias="StatusId")
    status_name: Optional[str] = Field(default=None, alias="StatusName")
    creation_date: Optional[Union[str, datetime]] = Field(default=None, alias="CreationDate")
    issue_date: Optional[Union[str, datetime]] = Field(default=None, alias="IssueDate")
    production_date: Optional[Union[str, datetime]] = Field(default=None, alias="ProductionDate")
    planned_production_date: Optional[Union[str, datetime]] = Field(default=None, alias="PlannedProductionDate")
    review_started: Optional[Union[str, datetime]] = Field(default=None, alias="ReviewStarted")
    product_name: Optional[str] = Field(default=None, alias="ProductName")
    theoretical_batch_yield: Optional[str] = Field(default=None, alias="TheoreticalBatchYield")
    batch_size: Optional[str] = Field(default=None, alias="BatchSize")
    batch_unit_name: Optional[str] = Field(default=None, alias="BatchUnitName")
    mpr_number: str = Field(..., alias="MPRNumber")
    mpr_version_number: str = Field(... ,alias="MPRVersionNumber")
    batch_number: Optional[str] = Field(None, alias="BatchNumber")
    production_number: Optional[str] = Field(default=None, alias="ProductionNumber")
    formulation_id: Optional[str] = Field(default=None, alias="FormulationId")
    part_number: Optional[str] = Field(default=None, alias="PartNumber")
    material_name: Optional[str] = Field(default=None, alias="MaterialName")
    material_version_number: Optional[str] = Field(default=None, alias="MaterialVersionNumber")
    material_type: Optional[str] = Field(default=None, alias="MaterialType")
    client_name: Optional[str] = Field(default=None, alias="ClientName")
    
    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values


class MasterProductionRecord(BaseModel):
    project_title: Optional[str] = Field(default=None, alias="ProjectTitle")
    mpr_version_id: Optional[str] = Field(default=None, alias="MPRVersionId")
    dms: Optional[str] = Field(default=None, alias="DMS")
    status_id: Optional[int] = Field(default=None, alias="StatusId")
    status_name: Optional[str] = Field(default=None, alias="StatusName")
    creation_date: Optional[Union[str, datetime]] = Field(default=None, alias="CreationDate")
    product_name: Optional[str] = Field(default=None, alias="ProductName")
    mpr_number: str = Field(..., alias="MPRNumber")
    mpr_version_number: str = Field(..., alias="MPRVersionNumber")
    client_id: Optional[str] = Field(default=None, alias="ClientId")
    client_name: Optional[str] = Field(default=None, alias="ClientName")
    author_person_id: Optional[str] = Field(default=None, alias="AuthorPersonId")
    author_person_full_name: Optional[str] = Field(default=None, alias="AuthorPersonFullName")
    formulation_id: Optional[str] = Field(default=None, alias="FormulationId")
    theoretical_batch_yield: Optional[str] = Field(default=None, alias="TheoreticalBatchYield")
    batch_size: Optional[str] = Field(default=None, alias="BatchSize")
    batch_unit_id: Optional[str] = Field(default=None, alias="BatchUnitId")
    batch_unit_name: Optional[str] = Field(default=None, alias="BatchUnitName")
    part_number: Optional[str] = Field(default=None, alias="PartNumber")
    material_name: Optional[str] = Field(default=None, alias="MaterialName")
    mat_version_number: Optional[str] = Field(default=None, alias="MatVersionNumber")
    material_type_id: Optional[str] = Field(default=None, alias="MaterialTypeId")
    material_type_name: Optional[str] = Field(default=None, alias="MaterialTypeName")
    reason_for_change: Optional[str] = Field(default=None, alias="ReasonForChange")

    @model_validator(mode='before')
    @classmethod
    def validate_fields(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
            if isinstance(value, str) and "0000-00-00" in value:
                values[key] = None
        return values
