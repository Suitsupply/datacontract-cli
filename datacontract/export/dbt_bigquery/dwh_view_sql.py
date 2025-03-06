"""Data Warehouse View SQL generator.

This module provides functionality to generate SQL for data warehouse views
with security configurations, field transformations, and model configurations.
It handles the generation of dbt view models with security filters, CTEs,
and field aliasing.

Example:
    contract = DataContract(...)
    sql_content = dwh_view_sql(contract)
"""

def dwh_view_sql(contract):
    """Generate SQL for a data warehouse view model.

    This function creates a SQL view definition including:
    - dbt config block with materialization settings
    - Security CTEs and filters
    - Field transformations and aliasing
    - Source reference configuration

    Args:
        contract: A DataContract object containing:
            - security_filters: SQL WHERE clauses for row-level security
            - config_meta: Model metadata configuration
            - config_tags: Model tags
            - config_labels: Model labels
            - column_data: List of fields with security and aliasing info
            - entity_label: Name/alias of the model
            - security_ctes: Common Table Expressions for security

    Returns:
        str: A formatted SQL string containing the complete view definition
            including config block, CTEs, and field selections.
    """
    security_filters = contract.security_filters

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
        select * from {{{{ ref('{contract.entity_label}') }}}}
    )
    select
        {secured_fields}
    from _source
    {security_filters}
    """
    return sql
