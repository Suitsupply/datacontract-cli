

import yaml
from datacontract.model.data_contract_specification import Model, Field


def dwh_view_sql(data_contract_model: Model) -> str:

    filters = data_contract_model.config['security_filters']

    config_meta = f",meta={data_contract_model.config['meta']}"
    config_tags = f",tags={data_contract_model.tags}"
    config_labels = ""
    if data_contract_model.config:
        config_labels = f",labels={data_contract_model.config}"


    fields = ""
    for field_key, field in data_contract_model.fields.items():
        if fields:
            fields += '\n\t\t,'
        fields += f"{field.config['secured_value']} as {field.title}"


    config_block = f"""
    {{{{
        config(
            materialized = 'view'
            ,alias = f"{data_contract_model.config['entity_label']}"
            {config_meta}
            {config_tags}
            {config_labels}
        )
    }}}}
    """
    # Final SQL Query
    sql = f"""
    {config_block}

    with
    {data_contract_model.config['security_ctes']}
    ,_source as (
        select * from {{{{ ref('{data_contract_model.config['entity_label']}') }}}}
        
    )
    select
        {fields}
    from _source
    {filters}
    """
    return sql
