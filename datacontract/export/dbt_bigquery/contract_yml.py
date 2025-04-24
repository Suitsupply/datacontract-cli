
import yaml
from datacontract.model.data_contract_specification import Model
from datacontract.model.config_dbt import ModelConfigDBT, FieldConfigDBT


def raw_contract_yml(data_contract_model: Model) -> str:
    model_name = f"raw__{data_contract_model.config['extended']['entity_label']}"
    return contract_yml(data_contract_model=data_contract_model, model_name=model_name)


def source_contract_yml(data_contract_model: Model) -> str:
    model_name = f"source__{data_contract_model.config['extended']['entity_label']}"    
    return contract_yml(data_contract_model=data_contract_model, model_name=model_name)


def dwh_view_yml(data_contract_model: Model) -> str:
    model_name = f"{data_contract_model.config['extended']['entity']}_view"    
    return contract_yml(data_contract_model=data_contract_model, model_name=model_name)


def contract_yml(data_contract_model: Model, model_name: str) -> str:

    model_config = ModelConfigDBT.model_validate(data_contract_model.config)

    fields = []
    for field_key, field in data_contract_model.fields.items():
        
        field_config = FieldConfigDBT.model_validate(field.config)

        constraints = None

        tests = None
        if model_name.startswith("raw__") and field.required and field_config.extended.category == 'data':
            tests = [{"not_null":{"where":"_loaded_at >= timestamp_add(current_timestamp, interval -3 day)"}}]
            
        policy_tags = None
        if field.pii:
            policy_tag = "{{ var('policy_tag_pii_prd') if target.name in ['prd'] else var('policy_tag_pii_dev')}}"
            policy_tags = [policy_tag]

        column_meta = None
        if field_config.security:
            column_meta = {}
            column_meta['security'] = field_config.security
        
        column = {
            "name": field.title
            ,"description": field.description.strip() if field.description else field.description
            ,"data_type": field_config.bigqueryType
            ,"data_tests": tests
            ,"constraints": constraints
            ,"policy_tags": policy_tags
            ,"meta": column_meta
        }

        fields.append(column)

    model_tests = []  

    config = {}
    config['alias'] = model_config.extended.entity_label
    config['tags'] = data_contract_model.tags
    config['labels'] = model_config.labels
    
    model_data = {
        "version": 2
        ,"models":[{
            "name": model_name
            ,"config": config
            ,"description": data_contract_model.description.strip() if data_contract_model.description else data_contract_model.description
            ,"meta": model_config.meta
            ,"data_tests": model_tests
            ,"columns": fields
        }]
    }
    clean_model = clean_yaml_nones(model_data)
    return yaml.dump(data=clean_model, allow_unicode=True, sort_keys=False)


def clean_yaml_nones(value):
    if isinstance(value, list):
        return [clean_yaml_nones(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {
            key: clean_yaml_nones(val)
            for key, val in value.items()
            if val is not None
        }
    else:
        return value
