from datacontract.model.data_contract_specification import Model
from datacontract.model.config_dbt import ModelConfigDBT

import yaml

def datasource_yml(data_contract_model: Model) -> str:

    model_config = ModelConfigDBT.model_validate(data_contract_model.config)

    recency_threshold = model_config.recencyThreshold if model_config.recencyThreshold else 0
    recency_field = model_config.recencyField if model_config.recencyField else '_loaded_at'

    recency_query = f'''(
            SELECT 
                GREATEST(
                    MAX({recency_field}),
                    MAX(COALESCE(PARSE_TIMESTAMP('%Y-%m-%d', review_data.reviewed_at), TIMESTAMP('1970-01-01')))
                ) as loaded_at
            FROM {model_config.extended.sql_table_name} as source_data
            LEFT JOIN `{model_config.extended.source_project}.elementary.dbt_source_freshness_reviews` as review_data
                ON review_data.table_id = '{model_config.extended.source_dataset}.{model_config.extended.source_table}'
        )
    '''
    
    freshness = { "warn_after": {"count": recency_threshold, "period": "day"} }
    if recency_threshold <= 0:
        freshness = None

    source_data = {
        "version": 2,
        "sources": [{
            "name": model_config.extended.source_dataset,
            "project": model_config.extended.source_project,
            "loader": model_config.loader,
            "tables": [{
                "name": f"{model_config.extended.entity_label}",
                "identifier": model_config.extended.source_table,
                "meta": model_config.meta,
                "tags": data_contract_model.tags,
                "labels": model_config.labels,
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
