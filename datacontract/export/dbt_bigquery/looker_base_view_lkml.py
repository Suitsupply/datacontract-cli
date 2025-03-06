"""Looker LookML View Generator.

This module provides functionality to generate LookML view files for Looker,
including field definitions, dimension configurations, and SQL table references.
It handles type mappings, timeframe configurations, and field transformations.

Example:
    contract = DataContract(...)
    lookml_content = looker_base_view_lkml(contract)
"""

import yaml
import lkml

FIELD_TYPE_MAPPING = {
    "INTEGER": "number",
    "INT64": "number",
    "FLOAT": "number",
    "FLOAT64": "number",
    "BIGNUMERIC": "number",
    "NUMERIC": "number",
    "BOOLEAN": "yesno",
    "BOOL": "yesno",
    "TIMESTAMP": "time",
    "TIME": "string",
    "DATE": "time",
    "DATETIME": "time",
    "STRING": "string",
    "ARRAY": "string",
    "GEOGRAPHY": "string",
    "BYTES": "string",
}


TIMEFRAME_TIME_GROUP = [
        "time",
        "time_of_day",
        "hour",
        "hour_of_day",
        "minute",
        "second",
        "millisecond",
        "microsecond",
    ]

FIELD_TYPE_TRANSFORM = {
    "TIME": ["cast(", " as string)"]    
}

CONFIG_OPTIONS = {
    "primary_key_columns": ["id", "pk", "primary_key"],
    "timeframes": ["raw", "date", "time", "minute30", "week", "month", "quarter", "year"],
    "time_suffixes": ["_at", "_date", "_time", "_ts", "_timestamp", "_datetime"],
}


def clean_yaml_nones(value):
    """Recursively remove None values from dictionaries and lists.

    Args:
        value: Input value to clean (dictionary, list, or other type)

    Returns:
        dict|list: Cleaned data structure with None values removed
        any: Original value if not a dictionary or list

    Example:
        >>> clean_yaml_nones({'a': None, 'b': 1})
        {'b': 1}
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
    
    
def get_dimension(contract_field):
    """Generate LookML dimension configuration from a contract field.

    Processes field metadata to generate appropriate LookML dimension
    configurations including:
    - Field type mapping
    - Time dimension handling
    - Primary key identification
    - Field descriptions and labels
    - SQL references

    Args:
        contract_field: Field specification from the data contract containing:
            - field_alias: The field name/alias
            - field_type: BigQuery data type
            - field_description: Field documentation
            - field_meta: Additional field metadata

    Returns:
        tuple: (dimension_type, field_config) where:
            - dimension_type: 'dimension' or 'dimension_group'
            - field_config: Dictionary of LookML field configurations
    """
    field = {}
    dimension_type = 'dimension'

    field_name = contract_field.field_alias
    field_sql_name = field_name
    field_type = contract_field.field_type
    
    lookml_type = FIELD_TYPE_MAPPING.get(field_type, "string")
    lookml_type_transform = FIELD_TYPE_TRANSFORM.get(field_type, ["",""])

    field_description = contract_field.field_description
    field_description = field_description.rstrip().replace('"', "'") if field_description else ''

    primary_key = None
    if contract_field.field_meta.get('primary_key', False):
        primary_key = "yes"
    
    hidden = None
    if contract_field.field_meta.get('hidden', False):
        hidden = "yes"
    
    group_label = contract_field.field_meta.get('group_label', None)
    
    datatype = None
    convert_ts = None

    if field_type == "DATE":
        convert_ts = "no"
        datatype = "date"
    elif field_type == "DATETIME":
        datatype = "datetime"


    if lookml_type == "time":
        timeframes = CONFIG_OPTIONS.get('timeframes', []).copy()
        
        if field_type == "DATE":
            timeframes = [timeframe for timeframe in timeframes if not any(timeframe.startswith(time_group) for time_group in TIMEFRAME_TIME_GROUP)]

        for s in CONFIG_OPTIONS.get('time_suffixes', []):
            if field_name.endswith(s):
                field_name = "_".join(field_name.split("_")[:-1])
                break

        dimension_type = "dimension_group"
        field = {
            "name": field_name
            ,"hidden": hidden
            ,"group_label": group_label
            ,"description": field_description
            ,"type": lookml_type
            ,"timeframes": timeframes
            ,"convert_tz": convert_ts
            ,"datatype": datatype
            ,"sql": f"{lookml_type_transform[0]}${{TABLE}}.{field_sql_name}{lookml_type_transform[1]}"
        }

    else:
        field = {
            "name": field_name
            ,"hidden": hidden
            ,"primary_key": primary_key
            ,"group_label": group_label
            
            ,"description": field_description
            ,"type": lookml_type
            ,"sql": f"{lookml_type_transform[0]}${{TABLE}}.{field_sql_name}{lookml_type_transform[1]}"
        }

    return dimension_type, field


def looker_base_view_lkml(contract, override_dataset_id: str = None):
    """Generate LookML view file content.

    Creates a complete LookML view definition including:
    - View name and SQL table reference
    - Dimensions and dimension groups
    - Field configurations and SQL references

    Args:
        contract: DataContract object containing:
            - entity_label: View name
            - project: Project ID
            - dataset: Dataset name
            - column_data: List of field specifications
        override_dataset_id: Optional dataset ID override

    Returns:
        str: Formatted LookML content for the view file

    Example:
        >>> content = looker_base_view_lkml(contract)
        >>> print(content)
        view: my_view {
          sql_table_name: `project.dataset.table` ;;
          dimension: field_name {
            type: string
            sql: ${TABLE}.field_name ;;
          }
        }
    """
    dimensions = []
    dimension_groups = []

    for contract_column in contract.column_data:
        dimension_type, field = get_dimension(contract_column)
        if dimension_type == 'dimension':
            dimensions.append(field)
        else:
            dimension_groups.append(field)

    dimensions = sorted(dimensions, key=lambda x: x['name'])
    dimension_groups = sorted(dimension_groups, key=lambda x: x['name'])

    source_data = {
        "views":[{
            "name": contract.entity_label
            ,"sql_table_name": f"`{contract.project}.{override_dataset_id.dataset if override_dataset_id else contract.dataset}.{contract.entity_label}`"
            ,"dimensions": dimensions
            ,"dimension_groups": dimension_groups
        }]
    }

    clean_model = clean_yaml_nones(source_data)

    return lkml.dump(clean_model)
    #return yaml.dump(data=clean_model, allow_unicode=True, sort_keys=False)
