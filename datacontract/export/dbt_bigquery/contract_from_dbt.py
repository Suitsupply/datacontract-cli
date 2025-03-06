"""DBT Contract Generator Module.

This module provides functionality to generate data contracts from dbt model files.
It handles the conversion of dbt model specifications into Contract objects,
including field transformations, documentation references, and security configurations.

Example:
    contract = ContractFromDBT(
        dbt_model_yaml="...",
        security_yaml="...",
        dbt_model_folder="datawarehouse",
        target="prd",
        docs={}
    )
"""

import yaml, json
import re
from .contract import Contract

class Object:
    """Simple JSON serializable object.
    
    Provides JSON serialization for objects used in contract generation.
    """
    
    def toJSON(self):
        """Convert object to JSON string.
        
        Returns:
            str: JSON representation of the object
        """
        return json.dumps(self, default=vars)

class ContractFromDBT(Contract):
    """Generate data contracts from dbt model specifications.

    This class extends the base Contract class to handle dbt-specific model
    configurations and convert them into standardized data contracts.

    Attributes:
        DBT_FOLDER_MAPPING: Dictionary mapping dbt folders to project configurations:
            - datawarehouse: Production and development DWH configurations
            - marts: Production and development marts configurations

    Args:
        dbt_model_yaml: YAML string containing dbt model specification
        security_yaml: Optional YAML string with security configurations
        dbt_model_folder: Folder type ('datawarehouse' or 'marts')
        target: Environment target ('prd' or 'dev')
        docs: Dictionary of documentation blocks

    Methods:
        get_full_description: Resolve documentation references in descriptions
        __init__: Initialize contract from dbt model specification
    """

    DBT_FOLDER_MAPPING = {
        "datawarehouse": {
            "prd": {
                "project": "pj-suitsupply-data-dwh-prd",
                "dataset": "dwh_view"
            },
            "dev": {
                "project": "pj-suitsupply-data-dwh-dev",
                "dataset": "dwh_view"
            }
        },
        "marts": {
            "prd": {
                "project": "pj-suitsupply-data-dwh-prd",
                "dataset": "marts"
            },
            "dev": {
                "project": "pj-suitsupply-data-dwh-dev",
                "dataset": "marts"
            }
        }
    }


    def get_full_description(self, description):
        """Resolve documentation references in descriptions.

        Args:
            description (str): Description with potential documentation references.

        Returns:
            str: Full description with resolved documentation references.
        """
        if description:
            match = re.search(r"{{\s*doc\(['\"](.+?)['\"]\)\s*}}", description)
            if match:
                doc_key = match.group(1)
                if doc_key in self.docs:
                    return self.docs[doc_key]
            
        return description


    def __init__(self, dbt_model_yaml, security_yaml = None, dbt_model_folder = 'datawarehouse', target = 'prd', docs = {}):
        """Initialize contract from dbt model specification.

        Args:
            dbt_model_yaml (str): YAML string containing dbt model specification.
            security_yaml (str, optional): YAML string with security configurations.
            dbt_model_folder (str): Folder type ('datawarehouse' or 'marts').
            target (str): Environment target ('prd' or 'dev').
            docs (dict): Dictionary of documentation blocks.
        """
        # reset class attributes
        self.reset_attributes()        

        dbt_model = yaml.safe_load(dbt_model_yaml).get('models')[0]
        self.docs = docs

        self.entity = f"{dbt_model.get('name')}"

        self.description = self.get_full_description(dbt_model.get('description'))

        self.entity_label = dbt_model.get('name')

        self.ownership_team = 'IT-MI'
        self.ownership_email = 'IT-ManagementInformation@suitsupply.com' 

        self.config_meta = dbt_model.get('meta', {})
        self.config_tags = dbt_model.get('config', {}).get('tags', [])        

        self.security = dbt_model.get('meta', {}).get('security', None)

        if dbt_model_folder not in self.DBT_FOLDER_MAPPING:
            raise Exception(f"Attribute '{dbt_model_folder}' is not supported.")
        if target not in ['dev', 'prd']:
            raise Exception(f"Target '{target}' is not supported.")

        self.project = self.DBT_FOLDER_MAPPING.get(dbt_model_folder,{}).get(target, {}).get('project')
        self.dataset = self.DBT_FOLDER_MAPPING.get(dbt_model_folder,{}).get(target, {}).get('dataset')

        default_mode = Object()
        default_mode.name = 'NULLABLE'
        default_mode.value = default_mode.name

        source_fields = []
        for column in dbt_model.get('columns'):

            # Ignore meta columns
            if column.get('name').startswith('_'):
                continue

            field = Object()
            field.name= column.get('name')
            field.description = self.get_full_description(column.get('description'))

            type = Object()
            type.name = column.get('data_type', 'unknown')
            if type.name in ['JSON', 'RECORD']:
                type.name = 'STRING'
            type.value = type.name
            field.type = type

            field.mode = default_mode
            field.calculated = False
            field.alias = None
            field.condition = None
            field.base_field = None
            field.contains_pii = False
            field.security = column.get('meta', {}).get('security', None)
            field.meta = column.get('meta', {})

            source_fields.append(field)

    
        self.generate_source_fields(
            field_category='data',
            fields=source_fields)
        
        self.set_security(security_yaml=security_yaml)
