"""Source Contract SQL Generator.

This module generates SQL queries for source dbt models based on data contracts.
It handles the generation of SQL including security configurations, field transformations,
and model configurations. The module creates views that apply security filters and
field-level security transformations on top of raw models.

Example:
    contract = DataContract(...)
    sql_content = source_contract_sql(contract)
"""

def source_contract_sql(contract):
    """Generate SQL for a source dbt model.

    Creates a complete SQL query with:
    - dbt config block with view materialization
    - Security CTEs and filters
    - Field-level security transformations
    - Raw model reference
    - Soft delete handling

    Args:
        contract: A DataContract object containing:
            - entity_label: Model name/alias
            - security_filters: SQL WHERE clauses for security
            - security_ctes: Common Table Expressions for security
            - has_deleted_flag: Boolean for soft delete handling
            - config_meta: Model metadata configuration
            - config_tags: Model tags
            - config_labels: Model labels
            - column_data: List of fields with security transformations
            - raw_model: Name of the referenced raw model

    Returns:
        str: A formatted SQL string containing:
            - dbt configuration block
            - Security CTEs
            - Source CTE with raw model reference
            - Field selections with security transformations
            - Security and soft delete filters

    Notes:
        - Handles field-level security transformations
        - Supports soft delete filtering
        - Applies row-level security filters
        - Maintains field aliases for consistency
    """
    security_filters = contract.security_filters  
    if contract.has_deleted_flag:
        security_filters += "\n    and not _deleted"

    config_meta = f",meta={contract.config_meta}"
    config_tags = f",tags={contract.config_tags}"
    config_labels = ""
    if contract.config_labels:
        config_labels = f",labels={contract.config_labels}"

    secured_fields = ""
    for field in contract.column_data:
        if secured_fields:
            secured_fields += '\n\t\t,'
        if field.secured_value and field.secured_value != field.field_alias:
            secured_fields += f"{field.secured_value} as {field.field_alias}"
        else:
            secured_fields += field.field_alias


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
    # Final SQL Query
    sql = f"""
    {config_block}

    with
    {contract.security_ctes}
    ,_source as (
        select * from {{{{ ref('{contract.raw_model}') }}}}
    )
    select
        {secured_fields}
    from _source
    {security_filters}
    """
    return sql
