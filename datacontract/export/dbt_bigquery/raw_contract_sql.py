from datacontract.model.data_contract_specification import Model


def raw_contract_sql(data_contract_model: Model) -> str:

    metadata_fields = ''
    snapshot_skip_fields = ''
    fields = ''
    primary_key = ''
    incremental_filter = ''
    deduplication_filter = ''

    for contract_field_key, contract_field in data_contract_model.fields.items():
        
        if contract_field.config['category'] == 'metadata':
            if metadata_fields:
                metadata_fields += '\n        ,'
            metadata_fields += f'{contract_field.config['source']:<54} as {contract_field.title}'  
        
        elif contract_field.config['category'] == 'data':
            if fields:
                fields += '\n        ,'
            fields += f'{contract_field.config['source']:<54} as {contract_field.title}'
        
        elif contract_field.config['category'] == '_primary_key':
            primary_key = f',{contract_field.config['source']} as {contract_field.title}'

        if contract_field.config.get('bigqueryType', '').lower() == 'json':
            if snapshot_skip_fields:
                snapshot_skip_fields += ','
            snapshot_skip_fields += contract_field.title

    config_post_hook = ""
    if data_contract_model.dbt.snapshot:
        config_post_hook = f',post_hook="{{{{create_external_snapshot(this, \'{snapshot_skip_fields}\' )}}}}"'

    partition_expiration = ""
    if data_contract_model.dbt.partitionExpirationDays:
        partition_expiration = f"\n\t\t\t\t,partition_expiration_days = {data_contract_model.dbt.partitionExpirationDays}"

    if primary_key and data_contract_model.dbt.deduplicate:
        deduplication_filter = f"qualify row_number()over(partition by _primary_key order by {data_contract_model.dbt.orderBy} desc) = 1"

    if  data_contract_model.dbt.incremental and \
        data_contract_model.fields['_primary_key'].config['source']:
        
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
                ,cluster_by = {data_contract_model.dbt.clusterBy}
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
        
        if data_contract_model.config['sql_where']:
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
        from {{{{ source('{data_contract_model.config['source_dataset']}', '{data_contract_model.config['product']}__{data_contract_model.config['entity']}') }}}}
        {''.join(data_contract_model.config['sql_joins'])}
    )
    select
        *
    {primary_key}
    from _source_query
    {data_contract_model.config['sql_where']}
    {incremental_filter}
    {deduplication_filter}
    """

    return sql
