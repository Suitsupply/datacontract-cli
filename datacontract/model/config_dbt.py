from typing import Dict, List, Optional, Any
from datacontract.model.data_contract_specification import Field
import pydantic as pyd


class FilterDBT(pyd.BaseModel):
    field: str | None = None
    value: str | None = None
    operator: str = '='
    dev_only: bool | None = None


class ModelConfigDBTExtended(pyd.BaseModel):
    # Basic source properties
    source_table: str | None = None
    source_dataset: Optional[str] = "unknown"
    source_project: Optional[str] = "unknown"
    sql_table_name: str = None
    
    # Entity properties
    product: str | None = None
    entity: str | None = None
    entity_label: str | None = None
    
    # SQL properties
    sql_joins: List[str] = []
    sql_where: str = ""
    security_ctes: Optional[str] = ""
    security_filters: Optional[str] = ""


class ModelConfigDBT(pyd.BaseModel):
    enabled: bool | None = None
    frequency: str | None = 'DAILY'
    filters: List[FilterDBT] = None
    typeOverwrite: bool | None = None
    snapshot: bool | None = None
    deduplicate: bool | None = None
    orderBy: str | None = None
    clusterBy: List[str] = []
    incremental: bool | None = None
    security: str | None = None
    partitionExpirationDays: int | None = None
    recencyThreshold: int | None = None
    recencyField: str | None = None
    sourceTable: str | None = None
    ephemerals: Dict[str, Field] = None
    
    loader: Optional[str] = None
    labels: Dict[str, str] = {}  # Contains: source_table, source_dataset, snapshot_enabled, contains_pii
    meta: Dict[str, Any] = {}    # Contains: owner, owner_email, security


    # Generated config properties from data_contract specification import
    extended: Optional[ModelConfigDBTExtended] = None
    model_config = pyd.ConfigDict(
        extra="allow",
    )

class FieldConfigDBTExtended(pyd.BaseModel):
    # Field type properties
    is_json: Optional[bool] = False
    is_calculated: Optional[bool] = False
    # Field source properties
    source: Optional[str] = None
    secured_value: Optional[str] = None
    category: Optional[str] = None  # data, metadata, _primary_key
    

class FieldConfigDBT(pyd.BaseModel):
    enabled: bool | None = None
    index: int | None = -1
    calculation: str | None = None
    pivot: bool | None = None
    pivotKeyField: str | None = None
    pivotKeyFilter: str | None = None
    piovtValueField: str | None = None
    security: str | None = None
    bigqueryType: str | None = None
    meta: Dict[str, Any] = {}  # Contains: owner, owner_email, security
    ephemerals: Dict[str, "Field"] = {}
    
    # Generated config properties from data_contract specification import
    extended: Optional[FieldConfigDBTExtended] = None
    model_config = pyd.ConfigDict(
        extra="allow",
    )
