from typing import Dict, List

from datacontract.imports.importer import Importer
from datacontract.lint.resolve import resolve_data_contract_from_location
from datacontract.model.exceptions import DataContractException
from datacontract.export.bigquery_converter import map_type_to_bigquery

from datacontract.model.data_contract_specification import DataContractSpecification, Server, Field, Model, FieldConfigDBT, ModelConfigDBT, FilterDBT
from datacontract.model.data_contract_security import ConfigSecurity

import re

SECURITY_YAML = '''
ctes:
  - name: user_groups
    query: |-
      select userGroup from `pj-suitsupply-data-dwh-prd.config.user_groups` where userEmail = session_user()
security:
  - name: table_access_all
    query: |-
      true
  - name: table_access_limited
    query: case when
      'data_engineer' in ( select `userGroup` from `user_groups` ) or
      'administrator' in ( select `userGroup` from `user_groups` )
      then true else false end
  - name: table_access_finance
    query: case when
      'finance' in ( select `userGroup` from `user_groups` ) or
      'administrator' in ( select `userGroup` from `user_groups` )
      then true else false end
  - name: table_access_cs
    query: case when
      'cs' in ( select `userGroup` from `user_groups` ) or
      'administrator' in ( select `userGroup` from `user_groups` )
      then true else false end
column_security:
  - name: column_nullify
    query: case when
      'administrator' in ( select `userGroup` from `user_groups` )
      then {} else null end
  - name: column_mask_str
    query: case when
      'administrator' in ( select `userGroup` from `user_groups` )
      then {} else '********' end
  - name: column_mask_email
    query: case when
      'administrator' IN ( select `userGroup` from `user_groups` )
      then {} else '*@*.*' end
'''

class DbtSpecificationImporter(Importer):
    
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str = None, import_args: dict = {}
    ) -> DataContractSpecification:
        
        if source:
            return _to_dbt_specification_bigquery_from_source(source, model=import_args.get('model', None))

        return _to_dbt_specification_bigquery(data_contract_specification, model=import_args.get('model', None))
        

def quote_name(_field_name):
    pattern = '^[a-zA-Z0-9_.]+$'
    pattern_wq = '^`.+`$'
    if not _field_name:
        return _field_name
    if bool(re.match(pattern, _field_name)):
        return _field_name
    if bool(re.match(pattern_wq, _field_name)):
        return _field_name
    return f"`{_field_name.lower()}`"


def clean_name(_field_name):
    if not _field_name:
        return _field_name
    pattern = '[^a-zA-Z0-9_]'
    return re.sub(pattern, '_', _field_name.strip().replace('`','').lower())  


def _to_dbt_specification_bigquery_from_source(source, model=None) -> DataContractSpecification:

    try:
        source_data_contract_spec = DataContractSpecification().from_file(source)

    except Exception as e:
        raise DataContractException(
            type="schema",
            name="Parse data contract",
            reason=f"Failed to parse data contract from {source}",
            engine="datacontract",
            original_exception=e,
        )
    
    return _to_dbt_specification_bigquery(source_data_contract_spec, model=model)


def _to_dbt_specification_bigquery(source_data_contract_spec: DataContractSpecification, model=None) -> DataContractSpecification:

    if source_data_contract_spec.servers is None:
        source_data_contract_spec.servers = {}
    landing_server = source_data_contract_spec.servers.get('landing', Server())
    source_server = source_data_contract_spec.servers.get('source', Server())

    truncate_timestamp = source_server.type in ["cosmosdb"]

    dbt_specification = DataContractSpecification()

    dbt_specification.id = f"dbt_specification__source_data_contract_spec.id"
    dbt_specification.dataContractSpecification = '1.1.0'
    dbt_specification.info = source_data_contract_spec.info
    dbt_specification.info.title = source_data_contract_spec.info.title
    dbt_specification.servers = source_data_contract_spec.servers

    if not model or model == 'all':
        if len(source_data_contract_spec.models) > 1:
            raise DataContractException(
                type="schema",
                name="Import data contract",
                reason="Multiple models found in data contract. Please specify a model to import.",
                engine="datacontract",
            )

    for source_model_name, source_model in source_data_contract_spec.models.items():

        if model and model != 'all' and source_model_name != model:
            continue
        
        if not source_model.config:
            source_model.config = {}

        if not source_model.dbt:
            source_model.dbt = ModelConfigDBT()

        dbt_model_name = source_model.config.get("bigqueryTable", source_model_name)

        dbt_model = Model()
        dbt_model.description = source_model.description
        dbt_model.type = source_model.type
        dbt_model.title = dbt_model_name
        dbt_model.tags = source_model.tags
        dbt_model.config = {} if not source_model.config else source_model.config
        dbt_model.dbt = source_model.dbt
        if dbt_model.dbt.deduplicate:
            if not dbt_model.dbt.orderBy:
                dbt_model.dbt.orderBy = '_loaded_at'
        
        dbt_model.config['source_table'] = source_model_name
        dbt_model.config['source_dataset'] = landing_server.dataset if landing_server.dataset else 'unknown'
        dbt_model.config['source_project'] = landing_server.format if landing_server.format else landing_server.project if landing_server.project  else 'unknown'
        dbt_model.config['loader'] = landing_server.driver if landing_server.driver else 'unknown'

        dbt_model.config['product'] = source_data_contract_spec.info.title
        dbt_model.config['entity'] = source_model.title
        dbt_model.config['entity_label'] = f"{source_data_contract_spec.info.title}__{source_model.title}"

        dbt_model.config['sql_joins'] = []
        dbt_model.config['sql_where'] = ''
        dbt_model.config['security_masks'] = {}
        
        # set security settings
        if SECURITY_YAML:
            _set_security(dbt_model, SECURITY_YAML)

        # generate list of filters
        for filter in dbt_model.dbt.filters:
            if dbt_model.config['sql_where'] == '':
                sql_where_connector = '\n    where'
            else:
                sql_where_connector = '\n    and'

            env_filter = ''
            if filter.dev_only:
                env_filter = " {{' or true' if target.name in ['acc','prd'] else ''}}"

            dbt_model.config['sql_where'] += f"{sql_where_connector} ({filter.field} {filter.operator} {filter.value}{env_filter})"       

        dbt_model.config['labels'] = {}
        dbt_model.tags = []

        dbt_model.config['labels']['source_table'] = source_model_name.lower().replace('*', '-tableset')
        if (len(source_model_name) > 63):
            dbt_model.config['labels']['source_table'] = f"{source_model_name.lower()[:53]}-truncated"
        
        dbt_model.config['labels']['source_dataset'] = landing_server.dataset.lower() if landing_server.dataset else 'unknown'
        dbt_model.config['labels']['snapshot_enabled'] = 'yes' if dbt_model.dbt.snapshot else 'no'

        dbt_model.config['meta'] = {'owner': source_data_contract_spec.info.owner,'owner_email':  source_data_contract_spec.info.contact.email if source_data_contract_spec.info.contact else 'unknown'}
        dbt_model.config['meta']['security'] = source_model.dbt.security if source_model.dbt.security else 'table_access_all'

        if source_data_contract_spec.servicelevels and source_data_contract_spec.servicelevels.frequency:
            dbt_model.tags.append(source_data_contract_spec.servicelevels.frequency.interval)
        else:
            dbt_model.tags.append('DAILY')
        dbt_model.tags.extend([dbt_model.config['product'], dbt_model.config['loader']])
        

        if dbt_model.dbt.recencyThreshold == None:
            if  source_data_contract_spec.servicelevels and \
                source_data_contract_spec.servicelevels.freshness and \
                source_data_contract_spec.servicelevels.freshness.threshold:

                recency_threshold = source_data_contract_spec.servicelevels.freshness.threshold
                if recency_threshold.endswith('D'):
                    dbt_model.dbt.recencyThreshold = int(recency_threshold[:-1])
                elif recency_threshold.endswith('M'):
                    dbt_model.dbt.recencyThreshold = int(recency_threshold[:-1]) * 30
                elif recency_threshold.endswith('Y'):
                    dbt_model.dbt.recencyThreshold = int(recency_threshold[:-1]) * 365
                else:
                    raise DataContractException(
                        type="schema",
                        name="Recency Threshold Conversion",
                        reason=f"Unsupported recency threshold format: {recency_threshold}",
                        engine="datacontract",
                    )

        dbt_model.fields = {}

        # Common metadata from server ephemerals
        _get_dbt_fields_bigquery(
              fields=landing_server.ephemerals if landing_server.ephemerals else {}
            , dbt_fields=dbt_model.fields
            , joins=dbt_model.config['sql_joins']
            , security_masks=dbt_model.config['security_masks']
            , data_type_overwrite=True
            , truncate_timestamp=truncate_timestamp
            , field_category = 'metadata'
            )
        
        # Model ephemerals
        _get_dbt_fields_bigquery(
              fields=source_model.ephemerals if source_model.ephemerals else {}
            , dbt_fields=dbt_model.fields
            , joins=dbt_model.config['sql_joins']
            , security_masks=dbt_model.config['security_masks']
            , data_type_overwrite=dbt_model.dbt.typeOverwrite
            , truncate_timestamp=truncate_timestamp
            , field_category = 'data'
            )
        
        # Model fields
        _get_dbt_fields_bigquery(
              fields=source_model.fields
            , dbt_fields=dbt_model.fields
            , joins=dbt_model.config['sql_joins']
            , security_masks=dbt_model.config['security_masks']
            , data_type_overwrite=dbt_model.dbt.typeOverwrite
            , truncate_timestamp=truncate_timestamp
            , field_category = 'data'
            )

        #Primary key field
        # dbt_model.primaryKey = []
        # for field_key, field in dbt_model.fields.items():
        #     if field.primaryKey:
        #         dbt_model.primaryKey.append(field.title) 
        #         continue        

        primary_field = Field()
        primary_field.dbt = FieldConfigDBT()
        primary_field.title = '_primary_key'
        primary_field.required = True
        primary_field.type = 'string'
        primary_field.description = 'Primary key.'
        primary_field.dbt.calculation = f"{{{{ dbt_utils.generate_surrogate_key(['{"', '".join(source_model.primaryKey)}']) }}}}" if source_model.primaryKey else "''"

        _get_dbt_fields_bigquery(
              fields={"_primary_key": primary_field}
            , dbt_fields=dbt_model.fields
            , field_category = '_primary_key'
            )

        dbt_specification.models[source_model_name] = dbt_model

        dbt_model.config['labels']['contains_pii'] = 'no'
        for field_key, field in dbt_model.fields.items():
            if field.pii:
                dbt_model.config['labels']['contains_pii'] = 'yes'
                dbt_model.tags.append('pii')
                break

    if model and model != 'all' and not dbt_specification.models:
        raise DataContractException(
            type="schema",
            name="Import data contract",
            reason=f"Model '{model}' not found in data contract",
            engine="datacontract",
        )
    if not dbt_specification.models:
        raise DataContractException(
            type="schema",
            name="Import data contract",
            reason=f"No models found in data contract",
            engine="datacontract",
        )

    return dbt_specification    
    
    
def _set_security(dbt_model : Model, security_yaml):
    
    dbt_model.config['security_masks'] = {}
    dbt_model.config['security_ctes'] = ''
    dbt_model.config['security_filters'] = ''
    
    if not security_yaml:
        return

    config_security = ConfigSecurity.from_string(security_yaml)

    # generate security cte
    for cte in config_security.ctes:
        if dbt_model.config['security_ctes']:
            dbt_model.config['security_ctes'] += f'\n    ,{cte.name} as ({cte.query})'
        else:
            dbt_model.config['security_ctes'] += f'\n    {cte.name} as ({cte.query})'   

    # generate security filters
    for filter in config_security.security:
        if filter.name == dbt_model.dbt.security:
            if dbt_model.config['security_filters'] == '':
                sql_where_connector = '\n    where'
            else:
                sql_where_connector = '\n    and'
            dbt_model.config['security_filters'] += f"{sql_where_connector} ({filter.query})"

    # generate security_masks
    for mask in config_security.column_security:
        dbt_model.config['security_masks'][mask.name] = mask.query
    
    if dbt_model.config['security_filters'] == '':
        dbt_model.config['security_filters'] = '\n    where false'


def _get_dbt_fields_bigquery(
        
        fields : Dict[str, Field],
        dbt_fields: Dict[str, Field],
        joins: List[str] = [],
        security_masks: Dict[str, str] = {},
        is_parent_json: bool = False,
        pivot_source_field: str = None,
        data_type_overwrite: bool = False,
        truncate_timestamp: bool = False,
        source_prefix: str = '',
        field_category: str = None
        ):


    for field_name, field in fields.items():

        if field.config is None:
            field.config = {}
            
        if field.dbt is None:
            field.dbt = FieldConfigDBT()

        if field.dbt.enabled == False:
            continue

        field_source = f"{source_prefix}{quote_name(field_name)}"

        is_calculated = False
        if field.dbt.calculation:
            is_calculated = True
            field_source = field.dbt.calculation

        field_title = clean_name(field.title) if field.title else clean_name(field_name)
        
        is_json = is_parent_json
        if not is_parent_json:
            is_json = field.config.get('bigqueryType', '').lower() == 'json'

        field.config['is_json'] = is_json
        field.config['is_calculated'] = is_calculated
        is_complex_field = False
        is_repeated = False
        nested_fields = {}

        if field.type == 'array' and len(field.items.fields) > 0:
            is_complex_field = True
            is_repeated = True
            nested_fields = field.items.fields
        elif field.type == 'record' and len(field.fields) > 0:
            is_complex_field = True
            nested_fields = field.fields

        #print(f"field: {field_name}, is_complex: {is_complex_field}, 'pivot': {field.dbt.pivot}")

        if is_complex_field:
            if not is_repeated and not field.dbt.pivot:
                nested_prefix = field_source + '.'
            elif is_repeated and field.dbt.index >= 0 and is_json:
                nested_prefix = f'{field_source}[{field.dbt.index}].'
            elif is_repeated and field.dbt.index >= 0:
                nested_prefix = f'{field_source}[safe_offset({field.dbt.index})].'                
            elif field.dbt.pivot:
                nested_prefix = ''
                #print('Pivot found')
                pivot_source_field = field_source
            else:
                nested_alias = f'nested_{field_title}'
                
                nested_prefix = f"{nested_alias}."
                json_unnest_prefix = ''
                if is_json:
                    json_unnest_prefix = 'json_extract_array'

                sql_join = f"left join unnest({json_unnest_prefix}({field_source})) as {nested_alias}\n"    
                joins.append(sql_join)

            # recursive execution of the function to extract nested attributes
            _get_dbt_fields_bigquery(
                  fields=nested_fields
                , dbt_fields=dbt_fields
                , joins=joins
                , security_masks=security_masks
                , is_parent_json=is_json
                , pivot_source_field=pivot_source_field
                , data_type_overwrite=data_type_overwrite
                , truncate_timestamp=truncate_timestamp
                , source_prefix=nested_prefix
                , field_category = field_category
                )
        else:

            #print(f"field: {field_name}, object: {pivot_source_field}")

            dbt_field = Field()
            dbt_field.title = field_title
            dbt_field.required = field.required
            dbt_field.primaryKey = field.primaryKey
            dbt_field.pii = field.pii
            dbt_field.classification = field.classification
            dbt_field.description = field.description
            dbt_field.config = field.config if field.config else {}

            dbt_field.type = field.type

            field_type = map_type_to_bigquery(field).upper()
            dbt_field.config['bigqueryType'] = field_type

            # Type conversion.
            if is_json and not is_calculated and field.dbt.pivotKeyFilter is None:
                #print(f"field: {field_name}, is_json: {is_json}, field_source: {field_source}")
                cast_field_source = _json_to_scalar_bigquery(field_source, field_type, truncate_timestamp=truncate_timestamp)
            elif not is_json and (is_calculated or data_type_overwrite) and field.dbt.pivotKeyFilter is None and field.title != '_primary_key':
                cast_field_source = f"cast( {field_source:<28}{' as ' + field_type + ' )':<20}"
            elif pivot_source_field is not None and field.dbt.pivotKeyFilter is not None:
                field_source_prefix = '__unnested.'
                if is_calculated:
                    field_source_prefix = ''

                pivot_field = f"{field_source_prefix}{field.dbt.pivotValueField}"
                json_unnest_prefix = ''
                sql_column_filter = f"(__unnested.{field.dbt.pivotKeyField}) = {field.dbt.pivotKeyFilter}"
                
                if is_json:
                    json_unnest_prefix = 'json_extract_array'
                    sql_column_filter = f"LAX_STRING(__unnested.{field.dbt.pivotKeyField}) = '{field.dbt.pivotKeyFilter.replace("'", "")}'"
                    pivot_field = _json_to_scalar_bigquery(pivot_field, field_type, truncate_timestamp = truncate_timestamp)

                cast_field_source = f"( select {pivot_field} from unnest({json_unnest_prefix}({pivot_source_field})) as __unnested where {sql_column_filter} )"
            else:
                cast_field_source = field_source

            field_source = cast_field_source 
            dbt_field.config['source'] = field_source

            dbt_field.dbt = field.dbt if field.dbt else FieldConfigDBT()
            dbt_field.tags = field.tags

            # Data masking rules to be applied to the field using DB views.
            field_secured_value = security_masks.get(field.dbt.security)
            if field_secured_value:
                field_secured_value = field_secured_value.replace('{}', field_title)

            dbt_field.config['secured_value'] = field_secured_value
            dbt_field.config['category'] = field_category
            
            dbt_fields[field_title] = dbt_field


def _json_to_scalar_bigquery(field_source, field_type, truncate_timestamp):
    #JSON type conversion functions in BigQuery use BOOL instead of BOOLEAN
    if field_type == 'BOOLEAN':
        field_type = 'BOOL'
    elif field_type == 'NUMERIC':
        field_type = 'FLOAT64'
    elif field_type in 'INTEGER':
        field_type = 'INT64' 

    cast_start = ""
    cast_end = ""  
    safe_cast_prefix = 'lax_'

    # Mapping between target data type and BigQuery function for JSON scalar conversion.
    bq_json_types = ["BOOL","INT64","FLOAT64","STRING"]
    
    # Convert all DATES
    if field_type in ("DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ") and truncate_timestamp:
        cast_start = f"{field_type.lower()}(nullif(left({safe_cast_prefix}string("
        cast_end = "), 26), ''))"
    elif field_type in ("DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ"):
        cast_start = f"{field_type.lower()}(nullif({safe_cast_prefix}string("
        cast_end = "), ''))"

    # Convert the field to one of the target formats, otherwise, return the field without data conversion (for example for JSON target format).
    elif field_type in bq_json_types:
        cast_start = f"{safe_cast_prefix}{field_type.lower()}("
        cast_end = ")"

    return f"{cast_start}{field_source}{cast_end}"
    
