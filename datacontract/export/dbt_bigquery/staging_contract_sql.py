from datacontract.model.data_contract_specification import Model

def staging_contract_sql(data_contract_model: Model) -> str:

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
            ,alias = '{data_contract_model.config['entity_label']}'
            {config_meta}
            {config_tags}
            {config_labels}
        )
    }}}}
    """

    fields = ""
    for field_key, field in data_contract_model.fields.items():
        if fields:
            fields += '\n\t\t,'
        fields += f"cast( {field.title:<28}{' as ' + field.config['bigqueryType'].lower() + ' )':<20}{ ' as ' + field.title}"

    # Final SQL Query
    sql = f"""
    {config_block}

    with 
    _source as (
        select * from {{{{ ref('raw__{data_contract_model.config['entity_label']}') }}}}
    )
    select
         {fields}
    from _source
    """
    return sql
