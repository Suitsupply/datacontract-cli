from datacontract.imports.importer import Importer

from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model
from datacontract.model.config_dbt import FieldConfigDBT, ModelConfigDBT

from datacontract.imports.sql_importer import map_type_from_sql

import re
import yaml
import os


class DbtModelImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
            
        return import_dbt_model(data_contract_specification, source=source)


def _get_doc_ref(description, documentation_yaml):
    
    if not documentation_yaml or not description:
        return description

    #Extracts a documentation reference from a description string.
    ref = re.search(r"{{\s*doc\(['\"](.+?)['\"]\)\s*}}", description)
    if ref:
        doc_key = ref.group(1)
        if doc_key in documentation_yaml:
            return documentation_yaml[doc_key]
    return description


def import_dbt_model(
        data_contract_spec: DataContractSpecification,
        source: str,
        ) -> DataContractSpecification: 

    with open(source, 'r') as file:
        source_file = yaml.safe_load(file)

    documentation_yaml = {}
    documentation_yaml_path = yaml.safe_load(os.environ.get('DOCUMENTATION_YAML_PATH', ''))
    if documentation_yaml_path:
        try:
            with open(documentation_yaml_path, 'r') as file:
                documentation_yaml = yaml.safe_load(file)
        except FileNotFoundError:
            documentation_yaml = {}

    # Process data contract models
    for dbt_model in source_file.get('models', []):

        dbt_model_name = dbt_model.get('name')
        
        data_contract_model = Model()
        model_config = ModelConfigDBT()

        data_contract_model.title = dbt_model.get('config', {}).get('alias', dbt_model_name)
        data_contract_model.description = _get_doc_ref(dbt_model.get('description'), documentation_yaml)

        model_config.security = dbt_model.get('meta', {}).get('security', None)
        model_config.meta = dbt_model.get('meta', {})

        data_contract_model.tags = dbt_model.get('config', {}).get('tags', [])

        dbt_fields = {}

        for column in dbt_model.get('columns'):

            dbt_field_name = column.get('name')
            field_config = FieldConfigDBT()

            dbt_field = Field()
            dbt_field.title = dbt_field_name
            dbt_field.description = _get_doc_ref(column.get('description', None), documentation_yaml)
            dbt_field.type = map_type_from_sql(column.get('data_type'))

            field_config.meta = column.get('meta', {})
            field_config.bigqueryType = column.get('data_type', 'unknown').upper()

            dbt_field_security = column.get('meta', {}).get('security')
            if dbt_field_security:
                field_config.security = dbt_field_security

            dbt_field.config = field_config.model_dump(exclude_none=True, exclude_defaults=True)

            dbt_fields[dbt_field_name] = dbt_field

        data_contract_model.fields = dbt_fields
        data_contract_model.config = model_config.model_dump(exclude_none=True, exclude_defaults=True)
        
        data_contract_spec.models[dbt_model_name] = data_contract_model

    return data_contract_spec    
