"""Staging Contract SQL Generator.

This module generates SQL queries for staging dbt models based on data contracts.
It handles the generation of SQL including field type casting, model configurations,
and reference to raw models. The module creates views that standardize data types
and field names from raw models.

Example:
    contract = DataContract(...)
    sql_content = staging_contract_sql(contract)
"""

def staging_contract_sql(contract):
    """Generate SQL for a staging dbt model.

    Creates a complete SQL query with:
    - dbt config block with view materialization
    - Model configuration (meta, tags, labels)
    - Field type casting
    - Raw model reference

    Args:
        contract: A DataContract object containing:
            - entity_label: Model name/alias
            - config_meta: Model metadata configuration
            - config_tags: Model tags
            - config_labels: Model labels
            - column_data: List of fields with:
                - field_alias: Field name
                - field_type: BigQuery data type
            - raw_model: Name of the referenced raw model

    Returns:
        str: A formatted SQL string containing:
            - dbt configuration block
            - Source CTE with raw model reference
            - Field selections with type casting
            - Field aliasing for consistency

    Notes:
        - Maintains consistent field naming
        - Standardizes data types through casting
        - Uses dbt ref functionality for dependencies
        - Formats SQL for readability
    """
    config_meta = f",meta={contract.config_meta}"
    config_tags = f",tags={contract.config_tags}"
    config_labels = ""
    if contract.config_labels:
        config_labels = f",labels={contract.config_labels}"

    config_block = f"""
    {{{{
        config(
            materialized = 'view'
            ,alias = '{contract.entity_label}'
            {config_meta}
            {config_tags}
            {config_labels}
        )
    }}}}
    """

    fields = ""
    for field in contract.column_data:
        if fields:
            fields += '\n\t\t,'
        fields += f"cast( {field.field_alias:<28}{' as ' + field.field_type.lower() + ' )':<20}{ ' as ' + field.field_alias}"

    # Final SQL Query
    sql = f"""
    {config_block}

    with 
    _source as (
        select * from {{{{ ref('{contract.raw_model}') }}}}
    )
    select
         {fields}
    from _source
    """
    return sql
