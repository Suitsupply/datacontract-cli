from datacontract.model.data_contract_specification import Model
from datacontract.model.config_dbt import ModelConfigDBT, FieldConfigDBT

def staging_contract_sql(data_contract_model: Model) -> str:

    model_config = ModelConfigDBT.model_validate(data_contract_model.config)

    config_meta = f",meta={model_config.meta}"
    config_tags = f",tags={data_contract_model.tags}"
    config_labels = ""
    if data_contract_model.config:
        config_labels = f",labels={model_config.labels}"

    fields = ""
    for field_key, field in data_contract_model.fields.items():

        field_config = FieldConfigDBT.model_validate(field.config)
        if fields:
            fields += '\n\t\t,'
        fields += f"{field_config.extended.secured_value} as {field.title}"

    config_block = f"""
    {{{{
        config(
            materialized = 'view'
            ,alias = '{model_config.extended.entity_label}'
            {config_meta}
            {config_tags}
            {config_labels}
        )
    }}}}
    """

    fields = ""
    for field_key, field in data_contract_model.fields.items():

        field_config = FieldConfigDBT.model_validate(field.config)
        if fields:
            fields += '\n\t\t,'
        fields += f"cast( {field.title:<28}{' as ' + field_config.bigqueryType.lower() + ' )':<20}{ ' as ' + field.title}"

    # Final SQL Query
    sql = f"""
    {config_block}

    with 
    _source as (
        select * from {{{{ ref('raw__{model_config.extended.entity_label}') }}}}
    )
    select
         {fields}
    from _source
    """
    return sql
