
from datacontract.model.data_contract_specification import Model
from datacontract.model.config_dbt import ModelConfigDBT, FieldConfigDBT


def dwh_view_sql(data_contract_model: Model) -> str:

    model_config = ModelConfigDBT.model_validate(data_contract_model.config)

    filters = model_config.extended.security_filters

    config_meta = f",meta={model_config.meta}"
    config_tags = f",tags={data_contract_model.tags}"

    fields = ""
    for field_key, field in data_contract_model.fields.items():

        field_config = FieldConfigDBT.model_validate(field.config)
        
        if fields:
            fields += '\n\t\t,'

        if field_config.extended.secured_value and field_config.extended.secured_value != field.title:
            fields += f"{field_config.extended.secured_value} as {field.title}"
        else:
            fields += field.title   


    config_block = f"""
    {{{{
        config(
            materialized = 'view'
            ,alias = '{model_config.extended.entity_label}'
            {config_meta}
            {config_tags}
            
        )
    }}}}
    """
    # Final SQL Query
    sql = f"""
    {config_block}

    with
    {model_config.extended.security_ctes}
    ,_source as (
        select * from {{{{ ref('{model_config.extended.entity_label}') }}}}
    )
    select
        {fields}
    from _source
    {filters}
    """
    return sql
