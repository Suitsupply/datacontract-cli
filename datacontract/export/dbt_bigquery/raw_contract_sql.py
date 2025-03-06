
def to_dbt_bigquery_raw_sql(contract):

    metadata_fields = ''
    snapshot_skip_fields = ''
    fields = ''
    primary_key = ''
    incremental_filter = ''
    deduplication_filter = ''

    for contract_field in contract.column_data:
        if contract_field.field_category == 'metadata':
            if metadata_fields:
                metadata_fields += '\n        ,'
            metadata_fields += f'{contract_field.field_source:<54} as {contract_field.field_alias}'  
        
        elif contract_field.field_category == 'data':
            if fields:
                fields += '\n        ,'
            fields += f'{contract_field.field_source:<54} as {contract_field.field_alias}'
        
        elif contract_field.field_category == 'primary_key':
            primary_key = f',{contract_field.field_source} as {contract_field.field_alias}'

        if contract_field.field_type == 'JSON':
            if snapshot_skip_fields:
                snapshot_skip_fields += ','
            snapshot_skip_fields += contract_field.field_alias

    config_post_hook = ""
    if contract.snapshot_status == 'ENABLED':
        config_post_hook = f',post_hook="{{{{create_external_snapshot(this, \'{snapshot_skip_fields}\' )}}}}"'

    partition_expiration = ""
    if contract.partition_expiration_days > 0:
        partition_expiration = f"\n\t\t\t\t,partition_expiration_days = {contract.partition_expiration_days}"

    if primary_key and contract.deduplication == 'ENABLED':
        deduplication_filter = f"qualify row_number()over(partition by _primary_key order by {contract.order_by} desc) = 1"

    if  contract.refresh_mode == 'INCREMENTAL' and \
        contract.has_primary_key:
        
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
                ,cluster_by = {contract.cluster_by}
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
        
        if contract.sql_where:
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
        ,{fields}        
        from {{{{ source('{contract.dataset}', '{contract.product}__{contract.entity}') }}}}
        {contract.sql_join}
    )
    select
        *
    {primary_key}
    from _source_query
    {contract.sql_where}
    {incremental_filter}
    {deduplication_filter}
    """

    return sql
