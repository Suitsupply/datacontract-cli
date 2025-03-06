import yaml
from datacontract.model.data_contract_dbt_bigquery import DataContract, DataContractColumn


def raw_contract_yml(contract: DataContract):
    
    return contract_yml(contract=contract, model=contract.raw_model)


def source_contract_yml(contract: DataContract):
    return contract_yml(contract=contract, model=contract.source_model)


def dwh_view_yml(contract: DataContract):
    return contract_yml(contract=contract, model=f"{contract.entity}_view")


def contract_yml(contract: DataContract, model: str):

    columns = []
    for contract_column in contract.column_data:
        
        constraints = None

        tests = None
        if model.startswith("raw__") and contract_column.field_mode == 'REQUIRED' and contract_column.field_category == 'data':
            tests = [{"not_null":{"where":"_loaded_at >= timestamp_add(current_timestamp, interval -3 day)"}}]
            
        policy_tags = None
        if contract_column.contains_pii:
            policy_tag = "{{ var('policy_tag_pii_prd') if target.name in ['prd'] else var('policy_tag_pii_dev')}}"
            policy_tags = [policy_tag]

        column_meta = None
        if contract_column.security:
            column_meta = {}
            column_meta['security'] = contract_column.security
        
        column = {
            "name": contract_column.field_alias
            ,"description": contract_column.field_description
            ,"data_type": contract_column.field_type
            ,"data_tests": tests
            ,"constraints": constraints
            ,"policy_tags": policy_tags
            ,"meta": column_meta
        }

        columns.append(column)

    model_tests = []  

    config = {}
    config['alias'] = contract.entity_label
    config['tags'] = contract.config_tags
    config['labels'] = contract.config_labels
    
    model_data = {
        "version": 2
        ,"models":[{
            "name": model
            ,"config": config
            ,"description": f"{contract.description}"
            ,"meta": contract.config_meta
            ,"data_tests": model_tests
            ,"columns":columns
        }]
    }
    clean_model = clean_yaml_nones(model_data)
    return yaml.dump(data=clean_model, allow_unicode=True, sort_keys=False)


def clean_yaml_nones(value: any):

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
