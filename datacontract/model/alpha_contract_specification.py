from __future__ import annotations
from pydantic import BaseModel, FieldValidationInfo, model_validator
from typing import Optional
from typing import Any, Dict
from enum import Enum
import yaml
import os
import json
import re

###############
# Enumerators #
###############

class FieldTypeEnum(str, Enum):
    STRING = 'STRING'
    INT64 = 'INTEGER'
    INTEGER = 'INTEGER'
    INT = 'INTEGER'
    BOOL = 'BOOLEAN'
    BOOLEAN = 'BOOLEAN'
    FLOAT64 = 'FLOAT'
    FLOAT = 'FLOAT'
    DATE = 'DATE'
    DATETIME = 'DATETIME'
    TIMESTAMP = 'TIMESTAMP'
    JSON = 'JSON'
    RECORD = 'RECORD'
    NUMERIC = 'NUMERIC'
    BIGNUMERIC = 'BIGNUMERIC'
    BYTES = 'BYTES'

class FieldModeEnum(str, Enum):
    REQUIRED = 'REQUIRED'
    NULLABLE = 'NULLABLE'

class OrderEnum(str, Enum):
    ASC = 'ASC'
    DESC = 'DESC'

class Enabled(str, Enum):
    ENABLED = 'ENABLED'
    DISABLED = 'DISABLED'

class FrequencyEnum(str, Enum):
    HOURLY = 'HOURLY'
    DAILY = 'DAILY'    
    WEEKLY = 'WEEKLY'
    MONTHLY = 'MONTHLY'
    DISABLED = 'DISABLED'
    TRIGGER_GA = 'TRIGGER_GA'

class RefreshModeEnum(str, Enum):
    INCREMENTAL = 'INCREMENTAL'
    LIVE = 'LIVE'
    FULL = 'FULL'

#################
# Model classes #
#################

class Ownership(BaseModel):
    class Config:
        extra = 'forbid' 
    team: str
    email: str

class FieldAlpha(BaseModel):
    class Config:
        extra = 'forbid' 
    name: str
    description: Optional[str] = None
    mode: Optional[FieldModeEnum] = FieldModeEnum.NULLABLE
    type: FieldTypeEnum
    fields: Optional[list[FieldAlpha]] = []
    alias: Optional[str] = None
    repeated: Optional[bool] = False
    index: Optional[int] = -1
    calculated: Optional[bool] = False
    pivot: Optional[bool] = False
    condition: Optional[str] = None
    base_field: Optional[str] = None
    contains_pii: Optional[bool] = False
    security: Optional[str] = None
    source_type: Dict[str, Any] | None = None

class Filter(BaseModel):
    class Config:
        extra = 'forbid' 
    field: str
    value: str
    operator: Optional[str] = '='
    dev_only: bool = False

class SourceSchema(BaseModel):
    class Config:
        extra = 'forbid' 
    primary_keys: Optional[list[str]] = []
    recency_validation: Optional[str] = None
    order_by: Optional[str] = None
    fields: list[FieldAlpha]
    filters: Optional[list[Filter]] = []

class RefreshPolicy(BaseModel):
    class Config:
        extra = 'forbid'   
    frequency: Optional[FrequencyEnum] = None
    data_type_overwrite: Optional[Enabled] = None 
    snapshot_status: Optional[Enabled] = None
    deduplication: Optional[Enabled] = None 
    refresh_mode: Optional[RefreshModeEnum] = None    
    cluster_by: Optional[list[str]] = []
    recency_threshold: Optional[int] = None
    partition_expiration_days: Optional[int] = None

class ConfigContractAlpha(BaseModel):
    class Config:
        extra = 'forbid' 
    entity: str
    description: str
    project: Optional[str] = None
    product: str
    dataset: Optional[str] = None
    identifier: Optional[str] = None
    contains_pii: Optional[bool] = False
    security: Optional[str] = None
    ownership: Ownership
    refresh_policy: Optional[RefreshPolicy] = RefreshPolicy()
    source_schema_root_field: Optional[FieldAlpha] = None
    source_schema: SourceSchema
    @model_validator(mode="after")  # Validate after all fields are initialized
    def validate_model(cls, values):
        # Validate entity and product naming convention
        if not re.match(r"^[a-z_][a-z0-9_]*$", values.entity):
            raise ValueError(
                f"The 'entity' must follow these naming rules: "
                f"only lowercase letters, numbers, and underscores are allowed, "
                f"and it must start with a letter or an underscore."
            )
        if not re.match(r"^[a-z_][a-z0-9_]*$", values.product):
            raise ValueError(
                f"The 'product' must follow these naming rules: "
                f"only lowercase letters, numbers, and underscores are allowed, "
                f"and it must start with a letter or an underscore."
            )
        # Additional checks for 'entity'
        if values.dataset == "datastream_mao" and values.entity.startswith("default_"):
            raise ValueError(
                f"For MAO tables, the 'entity' field cannot start with 'default_'. "
                f"The prefix '{'_'.join(values.entity.split('_')[:2])}_' should be excluded from the 'entity' value ('{values.entity}')."
            )
        return values
    @classmethod
    def from_file(cls, file):
        if not os.path.exists(file):
            raise Exception(f"The file '{file}' does not exist.")
        with open(file, "r", newline='') as file:
            file_content = file.read()
        return ConfigContractAlpha.from_string(file_content)
    @classmethod
    def from_string(cls, data_contract_str):
        data = yaml.safe_load(data_contract_str)
        return ConfigContractAlpha.model_validate(data)
    def to_yaml(self):
        return yaml.dump(self.model_dump(exclude_defaults=True, exclude_none=True), sort_keys=False, allow_unicode=True)    
# parsed = json.loads(ConfigContract.schema_json())
# print(json.dumps(parsed, indent=2))
