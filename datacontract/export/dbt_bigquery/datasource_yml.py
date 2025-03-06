"""Data source YAML configuration generator.

This module provides functionality to generate YAML configuration for data sources
in a dbt project. It handles the generation of source configurations including
freshness checks and loaded_at field queries with support for elementary freshness reviews.

Example:
    contract = DataContract(...)
    yaml_content = datasource_yml(contract)
"""

import yaml
from textwrap import dedent

def datasource_yml(contract):
    """Generate YAML configuration for a data source.

    This function creates a YAML configuration for a data source, including freshness checks
    and loaded_at field query. The query includes a join with the elementary freshness reviews
    table to consider manual review dates.

    Args:
        contract: A contract object containing source configuration details including:
            - recency_validation: Field used for freshness validation
            - project: Project ID
            - dataset: Dataset name
            - identifier: Table identifier
            - product: Product name
            - entity: Entity name
            - config_meta: Metadata configuration
            - config_tags: Tags configuration
            - config_labels: Labels configuration
            - recency_threshold: Threshold for freshness checks in days

    Returns:
        str: A YAML formatted string containing the source configuration with:
            - version
            - sources configuration including tables
            - freshness settings
            - loaded_at field with formatted SQL query
    """
    # Format the query with proper indentation and line breaks
    recency_query = f'''(
            SELECT 
                GREATEST(
                    MAX({contract.recency_validation}),
                    MAX(COALESCE(PARSE_TIMESTAMP('%Y-%m-%d', review_data.reviewed_at), TIMESTAMP('1970-01-01')))
                ) as loaded_at
            FROM `{contract.project}.{contract.dataset}.{contract.identifier}` as source_data
            LEFT JOIN `{contract.project}.elementary.dbt_source_freshness_reviews` as review_data
                ON review_data.table_id = '{contract.dataset}.{contract.identifier}'
        )
    '''
    
    freshness = { "warn_after": {"count": contract.recency_threshold, "period": "day"} }
    if contract.recency_threshold <= 0:
        freshness = None

    source_data = {
        "version": 2,
        "sources": [{
            "name": contract.dataset,
            "project": contract.project,
            "loader": contract.loader,
            "tables": [{
                "name": f"{contract.product}__{contract.entity}",
                "identifier": contract.identifier,
                "meta": contract.config_meta,
                "tags": contract.config_tags,
                "labels": contract.config_labels,
                "freshness": freshness,
                "loaded_at_field": "$recency_query"#
            }]
        }]
    }

    # Use dump with proper YAML formatting options
    return yaml.dump(
        clean_yaml_nones(source_data),
        allow_unicode=True,
        sort_keys=False,
        indent=2,
        width=120,
        default_style=None
    ).replace("$recency_query", recency_query)


def clean_yaml_nones(value):
    """Recursively remove None values from dictionaries and lists.

    This function walks through nested dictionaries and lists to remove None values,
    except for the 'freshness' key which is preserved even when None.

    Args:
        value: The input value to clean, can be a dictionary, list, or other type.

    Returns:
        The cleaned value with all None values removed from dictionaries and lists.
        If the input is not a dictionary or list, returns the input value unchanged.

    Example:
        >>> clean_yaml_nones({'a': None, 'b': 1, 'freshness': None})
        {'b': 1, 'freshness': None}
    """
    if isinstance(value, list):
        return [clean_yaml_nones(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {
            key: clean_yaml_nones(val)
            for key, val in value.items()
            if key == 'freshness' or val is not None
        }
    else:
        return value
