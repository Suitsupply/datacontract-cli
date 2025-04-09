from __future__ import annotations
from pydantic import BaseModel
from typing import Optional

import yaml
import os

class MetaElement(BaseModel):
    class Config:
        extra = 'forbid' 
    name: str
    query: str

class ConfigSecurity(BaseModel):
    class Config:
        extra = 'forbid' 
    
    ctes: Optional[list[MetaElement]] = []
    security: Optional[list[MetaElement]] = []
    column_security: Optional[list[MetaElement]] = []

    def from_file(cls, file):
        if not os.path.exists(file):
            raise Exception(f"The file '{file}' does not exist.")
        with open(file, "r", newline='') as file:
            file_content = file.read()
        return ConfigSecurity.from_string(file_content)

    @classmethod
    def from_string(cls, data_str):
        data = yaml.safe_load(data_str)
        return ConfigSecurity(**data)
    
    def to_yaml(self):
        return yaml.dump(self.model_dump(exclude_defaults=True, exclude_none=True), sort_keys=False, allow_unicode=True)    
    
# parsed = json.loads(ConfigContract.schema_json())
# print(json.dumps(parsed, indent=2))
