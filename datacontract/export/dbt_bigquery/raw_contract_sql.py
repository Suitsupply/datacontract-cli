from datacontract.model.data_contract_specification import Model
from datacontract.model.config_dbt import ModelConfigDBT, FieldConfigDBT


def raw_contract_sql(data_contract_model: Model) -> str:

    metadata_fields = ''
    snapshot_skip_fields = ''
    fields = ''
    primary_key = ''
    incremental_filter = ''
    deduplication_filter = ''

    model_config = ModelConfigDBT.model_validate(data_contract_model.config)

    for contract_field_key, contract_field in data_contract_model.fields.items():

        field_config = FieldConfigDBT.model_validate(contract_field.config)
        
        if field_config.extended.category == 'metadata':
            if metadata_fields:
                metadata_fields += '\n        ,'
            metadata_fields += f'{field_config.extended.source:<54} as {contract_field.title}'  
        
        elif field_config.extended.category == 'data':
            if fields:
                fields += '\n        ,'
            fields += f'{field_config.extended.source:<54} as {contract_field.title}'
        
        elif field_config.extended.category == '_primary_key':
            primary_key = f',{field_config.extended.source} as {contract_field.title}'

        if contract_field.config.get('bigqueryType', '').lower() == 'json':
            if snapshot_skip_fields:
                snapshot_skip_fields += ','
            snapshot_skip_fields += contract_field.title

    config_post_hook = ""
    if model_config.snapshot:
        config_post_hook = f',post_hook="{{{{create_external_snapshot(this, \'{snapshot_skip_fields}\' )}}}}"'

    partition_expiration = ""
    if model_config.partitionExpirationDays:
        partition_expiration = f"\n\t\t\t\t,partition_expiration_days = {model_config.partitionExpirationDays}"

    if primary_key and model_config.deduplicate:
        deduplication_filter = f"qualify row_number()over(partition by _primary_key order by {model_config.orderBy} desc) = 1"

    if  model_config.incremental and \
        data_contract_model.fields['_primary_key'].config['extended']['source']:
        
        config_block = f"""

        {{% if execute %}}
        {{% if flags.FULL_REFRESH %}}
            {{{{ exceptions.raise_compiler_error("Full refresh is not allowed for raw tables. Exclude physical tables with \'--exclude \' flag or drop the tables manually.") }}}}
        {{% endif %}}
        {{% endif %}}   
                
        {{{{
            config(
                materialized = 'incremental'
                ,on_schema_change = 'append_new_columns'
                ,partition_by = {{
                    'field': '_loaded_at',
                    'data_type': 'timestamp',
                    'granularity': 'day'
                }}
                ,cluster_by = {model_config.clusterBy}
                ,contract = {{
                    "enforced":True
                }}
                {config_post_hook}{partition_expiration}
            )            
        }}}}

        {{% if is_incremental() %}}
            {{%- set sql_statement -%}}
                (select max(_loaded_at) from {{{{ this }}}} WHERE _loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 7 DAY))
            {{%- endset -%}}
            {{%- set max_ts = dbt_utils.get_single_value(sql_statement, default=None) -%}}
            
            {{% if max_ts == None %}}

                {{%- set sql_statement %}}
                    (select max(_loaded_at) from {{{{ this }}}})
                {{% endset -%}}
                {{%- set max_ts = dbt_utils.get_single_value(sql_statement, default=None) -%}}

                {{% if max_ts == None %}}
                    {{%- set max_ts = "2020-01-01" %}}
                {{% endif -%}}
            {{% endif %}}
        {{% endif -%}}
        """
        
        if model_config.extended.sql_where:
            incremental_filter = f"{{% if is_incremental() %}}\n    and _loaded_at > TIMESTAMP('{{{{ max_ts }}}}')\n    {{% endif %}}"
        else:
            incremental_filter = f"{{% if is_incremental() %}}\n    where _loaded_at > TIMESTAMP('{{{{ max_ts }}}}')\n    {{% endif %}}"

    else:
        config_block = f"""
    {{{{
        config(
            materialized = 'view'
            {config_post_hook}
        )            
    }}}}
    """

    # Final SQL query content
    sql = f"""
    {config_block}
    with 
    _source_query as (
        select
        --Metadata
         {metadata_fields}

        --Source Data
        {',' if metadata_fields else ''}{fields}        
        from {{{{ source('{model_config.extended.source_dataset}', '{model_config.extended.entity_label}') }}}}
        {''.join(model_config.extended.sql_joins)}
    )
    select
        *
    {primary_key}
    from _source_query
    {model_config.extended.sql_where}
    {incremental_filter}
    {deduplication_filter}
    """

    return sql
