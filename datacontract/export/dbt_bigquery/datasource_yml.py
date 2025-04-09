from datacontract.model.data_contract_specification import Model

import yaml
from textwrap import dedent

def datasource_yml(data_contract_model: Model) -> str:


    recency_threshold = data_contract_model.dbt.recencyThreshold if data_contract_model.dbt.recencyThreshold else 0
    recency_field = data_contract_model.dbt.recencyField if data_contract_model.dbt.recencyField else '_loaded_at'

    recency_query = f'''(
            SELECT 
                GREATEST(
                    MAX({recency_field}),
                    MAX(COALESCE(PARSE_TIMESTAMP('%Y-%m-%d', review_data.reviewed_at), TIMESTAMP('1970-01-01')))
                ) as loaded_at
            FROM `{data_contract_model.config['source_project']}.{data_contract_model.config['source_dataset']}.{data_contract_model.config['source_table']}` as source_data
            LEFT JOIN `{data_contract_model.config['source_project']}.elementary.dbt_source_freshness_reviews` as review_data
                ON review_data.table_id = '{data_contract_model.config['source_dataset']}.{data_contract_model.config['source_table']}'
        )
    '''
    
    freshness = { "warn_after": {"count": recency_threshold, "period": "day"} }
    if recency_threshold <= 0:
        freshness = None

    source_data = {
        "version": 2,
        "sources": [{
            "name": data_contract_model.config['source_dataset'],
            "project": data_contract_model.config['source_project'],
            "loader": data_contract_model.config['loader'],
            "tables": [{
                "name": f"{data_contract_model.config['product']}__{data_contract_model.config['entity']}",
                "identifier": data_contract_model.config['source_table'],
                "meta": data_contract_model.config['meta'],
                "tags": data_contract_model.tags,
                "labels": data_contract_model.config['labels'],
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
