import re, yaml, os
from typing import Dict, List

from datacontract.export.exporter import Exporter
from datacontract.model.exceptions import DataContractException

from datacontract.model.dbt_data_contract_specification import DataContractSpecification
from datacontract.export.bigquery_converter import map_type_to_bigquery

from datacontract.model.data_contract_specification import DataContractSpecification, Server, Field, Model
from datacontract.model.config_dbt import ModelConfigDBT, FieldConfigDBT, ModelConfigDBTExtended, FieldConfigDBTExtended
from datacontract.model.config_security import ConfigSecurity

from datacontract.export.dbt_bigquery.datasource_yml import datasource_yml
from datacontract.export.dbt_bigquery.raw_contract_sql import raw_contract_sql
from datacontract.export.dbt_bigquery.contract_yml import raw_contract_yml
from datacontract.export.dbt_bigquery.source_contract_sql import source_contract_sql
from datacontract.export.dbt_bigquery.contract_yml import source_contract_yml
from datacontract.export.dbt_bigquery.staging_contract_sql import staging_contract_sql
from datacontract.export.dbt_bigquery.dwh_view_sql import dwh_view_sql
from datacontract.export.dbt_bigquery.contract_yml import dwh_view_yml
from datacontract.export.dbt_bigquery.looker_base_view_lkml import looker_base_view_lkml


CONTRACT_MAPPING = {
    'dbt-spec-data-source-yml': datasource_yml,
    'dbt-spec-raw-sql': raw_contract_sql,
    'dbt-spec-raw-yml': raw_contract_yml,
    'dbt-spec-source-sql': source_contract_sql,
    'dbt-spec-source-yml': source_contract_yml,
    'dbt-spec-staging-sql': staging_contract_sql,
    'dbt-spec-dwh-view-sql': dwh_view_sql,
    'dbt-spec-dwh-view-yml': dwh_view_yml,
    'dbt-spec-looker-base-view-lkml': looker_base_view_lkml,    
}

class DbtSpecificationExporter(Exporter):
    
    def export(self, data_contract: DataContractSpecification, model, server, sql_server_type, export_args) -> dict:
        if self.export_format == 'dbt-specification':
            return convert_to_dbt_specification(data_contract, server=server).model_dump(exclude_none=True, exclude_defaults=True)

        if self.export_format not in CONTRACT_MAPPING:
            raise DataContractException(
                type="schema",
                name="Export to dbt",
                reason=f"Export format {self.export_format} not supported.",
                engine="datacontract",
            )
        
        return to_dbt_file(data_contract=data_contract, model=model, format=self.export_format, server=server)


def to_dbt_file(data_contract: DataContractSpecification, format: str, server: str = 'landing', model: str = 'all', ) -> str:

    if data_contract.info.version != 'dbt_specification':
        convert_to_dbt_specification(data_contract, server=server)

    if model == "all":
        if not data_contract.models.keys():
            raise DataContractException(
                type="schema",
                name="Export to dbt",
                reason="No models found in data contract.",
                engine="datacontract",
            )
        process_model = next(iter(data_contract.models.keys()))
    else:
        if model not in data_contract.models:
            raise DataContractException(
                type="schema",
                name="Export to dbt",
                reason=f"Model {model} not found in data contract.",
                engine="datacontract",
            )        
        process_model = model

    if not process_model:
        raise DataContractException(
            type="schema",
            name="Export to dbt",
            reason="Unable to get processed model from data contract.",
            engine="datacontract",
        )

    func = CONTRACT_MAPPING.get(format)
    
    return func(data_contract.models[process_model])


def convert_to_dbt_specification(
        data_contract_spec: DataContractSpecification
        ,server: str = 'landing'
        ) -> DataContractSpecification:

    data_contract_spec.info.version = 'dbt_specification'

    # Add server data if missing
    data_server = data_contract_spec.servers.get(server, Server()) if data_contract_spec.servers else Server()
    
    # Get security masks from environment variable
    security_yaml = {}
    security_yaml_path = yaml.safe_load(os.environ.get('SECURITY_YAML_PATH', ''))
    if security_yaml_path:
        with open(security_yaml_path, 'r', encoding='utf-8') as file:
            security_yaml = yaml.safe_load(file)
    security_masks = {mask['name']: mask['query'] for mask in security_yaml.get('column_security', [])} if security_yaml else {}

    # Get the default model config from the template specification
    default_model = data_contract_spec.models.get('_default', Model())
    default_config = ModelConfigDBT.model_validate(default_model.config if default_model.config else {})

    # Process data contract models
    for model_name, data_contract_model in data_contract_spec.models.items():

        model_config = ModelConfigDBT.model_validate(data_contract_model.config if data_contract_model.config else {})
        default_ephemerals = {}

        # Merge default config with model config
        if default_config:
            for attr in ModelConfigDBT.__annotations__:

                if attr == 'filters':
                    if default_config.filters and model_config.filters:
                        model_config.filters = default_config.filters + model_config.filters
                    elif default_config.filters:
                        model_config.filters = default_config.filters
                elif attr == 'ephemerals':
                    if default_config.ephemerals:
                        default_ephemerals = default_config.ephemerals
                elif attr != 'alias':
                    default_val = ModelConfigDBT.model_fields[attr].default
                    if getattr(model_config, attr) == default_val and getattr(default_config, attr) != default_val:
                        setattr(model_config, attr, getattr(default_config, attr))

        if not data_contract_model.title:
            data_contract_model.title = model_name
        dbt_model_identifier = model_config.sourceTable if model_config.sourceTable else model_name

        if model_config.deduplicate:
            if not model_config.orderBy:
                model_config.orderBy = '_loaded_at'

        model_config.extended = ModelConfigDBTExtended()
        model_config.extended.source_table = dbt_model_identifier
        model_config.extended.source_dataset = data_server.dataset if data_server.dataset else 'unknown'
        model_config.extended.source_project = data_server.format if data_server.format else data_server.project if data_server.project  else 'unknown'
        model_config.extended.sql_table_name = f"`{model_config.extended.source_project}.{model_config.extended.source_dataset}.{dbt_model_identifier}`"
                
        model_config.loader = 'unknown' if not model_config.loader else model_config.loader
        model_config.extended.product = data_contract_model.namespace if data_contract_model.namespace else data_contract_spec.id
        model_config.extended.entity = data_contract_model.title

        model_config.extended.entity_label = model_config.extended.entity
            
        model_config.extended.sql_joins = []
        model_config.extended.sql_where = ''
        
        if server == 'landing' and not model_config.security:
            model_config.security = 'table_access_all'

        if security_yaml:
            _set_security(model_config, security_yaml)

        # generate list of filters
        if model_config.filters:
            for filter in model_config.filters:
                if model_config.extended.sql_where == '':
                    sql_where_connector = '\n    where'
                else:
                    sql_where_connector = '\n    and'

                env_filter = ''
                if filter.dev_only:
                    env_filter = " {{' or true' if target.name in ['acc','prd'] else ''}}"

                model_config.extended.sql_where += f"{sql_where_connector} ({filter.field} {filter.operator} {filter.value}{env_filter})"       

        if server == 'landing':
            model_config.extended.entity_label = f"{model_config.extended.product}__{model_config.extended.entity}"

            if not model_config.labels:
                model_config.labels = {}
            data_contract_model.tags = []

            model_config.labels['source_table'] = dbt_model_identifier.lower().replace('*', '-tableset')
            if (len(dbt_model_identifier) > 63):
                model_config.labels['source_table'] = f"{dbt_model_identifier.lower()[:53]}-truncated"
            
            model_config.labels['source_dataset'] = data_server.dataset.lower() if data_server.dataset else 'unknown'
            model_config.labels['snapshot_enabled'] = 'yes' if model_config.snapshot else 'no'

            if not model_config.meta:
                model_config.meta = {}
            if not model_config.meta.get('owner'):
                model_config.meta['owner'] = data_contract_spec.info.owner
            if not model_config.meta.get('owner_email'):
                model_config.meta['owner_email'] = data_contract_spec.info.contact.email if data_contract_spec.info.contact else 'unknown'
            
            model_config.meta['security'] = model_config.security
            
            data_contract_model.tags.append(model_config.frequency)
            
            data_contract_model.tags.extend([ model_config.loader, model_config.extended.product])

        if model_config.recencyThreshold == None:
            if  data_contract_spec.servicelevels and \
                data_contract_spec.servicelevels.freshness and \
                data_contract_spec.servicelevels.freshness.threshold:

                recency_threshold = data_contract_spec.servicelevels.freshness.threshold
                if recency_threshold.endswith('D'):
                    model_config.recencyThreshold = int(recency_threshold[:-1])
                elif recency_threshold.endswith('M'):
                    model_config.recencyThreshold = int(recency_threshold[:-1]) * 30
                elif recency_threshold.endswith('Y'):
                    model_config.recencyThreshold = int(recency_threshold[:-1]) * 365
                else:
                    raise DataContractException(
                        type="schema",
                        name="Recency Threshold Conversion",
                        reason=f"Unsupported recency threshold format: {recency_threshold}",
                        engine="datacontract",
                    )

        dbt_fields = {}
        truncate_timestamp = data_server.dataset == 'cosmosdb'
        
        # Server ephemerals (metadata)
        _get_dbt_fields_bigquery(
              fields=default_ephemerals
            , dbt_fields=dbt_fields
            , joins=model_config.extended.sql_joins
            , security_masks=security_masks
            , data_type_overwrite=model_config.typeOverwrite
            , truncate_timestamp=truncate_timestamp
            , field_category = 'metadata'
            , server = server
            )

        # Model ephemerals
        _get_dbt_fields_bigquery(
              fields=model_config.ephemerals if model_config.ephemerals else {}
            , dbt_fields=dbt_fields
            , joins=model_config.extended.sql_joins
            , security_masks=security_masks
            , data_type_overwrite=model_config.typeOverwrite
            , truncate_timestamp=truncate_timestamp
            , field_category = 'data'
            , server = server
            )
        
        # Model fields
        _get_dbt_fields_bigquery(
              fields=data_contract_model.fields
            , dbt_fields=dbt_fields
            , joins=model_config.extended.sql_joins
            , security_masks=security_masks
            , data_type_overwrite=model_config.typeOverwrite
            , truncate_timestamp=truncate_timestamp
            , field_category = 'data'
            , server = server
            )

        if server == 'landing':
            primary_field = Field()
            primary_field.config = FieldConfigDBT()
            primary_field.title = '_primary_key'
            primary_field.required = True
            primary_field.type = 'string'
            primary_field.description = 'Primary key.'
            if data_contract_model.primaryKey:
                keys = "', '".join(data_contract_model.primaryKey)
                primary_field.config.calculation = f"{{{{ dbt_utils.generate_surrogate_key(['{keys}']) }}}}"
            else:
                primary_field.config.calculation = "''"

            _get_dbt_fields_bigquery(
                fields={"_primary_key": primary_field}
                , dbt_fields=dbt_fields
                , field_category = '_primary_key'
                )

        data_contract_model.fields = dbt_fields

        model_config.ephemerals = None

        if server == 'landing':
            
            if not model_config.labels.get('contains_pii'):
            
              model_config.labels['contains_pii'] = 'no'

            #     for field_key, field in dbt_fields.items():
            #         if field.pii:
            #             model_config.labels['contains_pii'] = 'yes'
            #             break

            # Temporary reorder labels for internal validation
            contains_pii = model_config.labels.pop('contains_pii')
            if contains_pii == 'yes':
                data_contract_model.tags.append('contains_pii')
            model_config.labels['contains_pii'] = contains_pii

        data_contract_model.config = model_config.model_dump(exclude_none=True, exclude_defaults=True)
        data_contract_spec.models[model_name] = data_contract_model

    return data_contract_spec
    
    
def _set_security(model_config : ModelConfigDBT, security_yaml):
    
    model_config.extended.security_ctes = ''
    model_config.extended.security_filters = ''
    
    if not security_yaml:
        return

    config_security = ConfigSecurity.model_validate(security_yaml)

    # generate security cte
    for cte in config_security.ctes:
        if model_config.extended.security_ctes:
           model_config.extended.security_ctes += f'\n    ,{cte.name} as ({cte.query})'
        else:
            model_config.extended.security_ctes += f'\n    {cte.name} as ({cte.query})'   

    # generate security filters
    for filter in config_security.security:
        if filter.name == model_config.security:
            if model_config.extended.security_filters == '':
                sql_where_connector = '\n    where'
            else:
                sql_where_connector = '\n    and'
            model_config.extended.security_filters += f"{sql_where_connector} ({filter.query})"

    if model_config.extended.security_filters == '':
        model_config.extended.security_filters = '\n    where false'


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
        field_category: str = None,
        server: str = 'landing'
        ):


    for field_name, field in fields.items():

        field_config = FieldConfigDBT.model_validate(field.config if field.config else {})
        field_config.extended = FieldConfigDBTExtended()

        if server != 'landing' and field_name.startswith('_'):
            continue

        field_source = f"{source_prefix}{quote_name(field_name)}"

        is_calculated = False
        if field_config.calculation:
            is_calculated = True
            field_source = field_config.calculation

        field_title = clean_name(field.title) if field.title else clean_name(field_name)
        
        is_json = is_parent_json
        if not is_parent_json and field_config.bigqueryType:
            is_json = field_config.bigqueryType.lower() == 'json'

        field_config.extended.is_json = is_json
        field_config.extended.is_calculated = is_calculated
        is_complex_field = False
        is_repeated = False
        nested_fields = {}

        if field.type == 'array' and field_config.pivot:
            is_complex_field = True
            is_repeated = False
            nested_fields = field_config.ephemerals if field_config.ephemerals else {}
        elif field.type == 'array' and len(field.items.fields) > 0:
            is_complex_field = True
            is_repeated = True
            nested_fields = field.items.fields
        elif field.type == 'record' and len(field.fields) > 0:
            is_complex_field = True
            nested_fields = field.fields

        if is_complex_field:
            if not is_repeated and not field_config.pivot:
                nested_prefix = field_source + '.'
            elif is_repeated and field_config.index >= 0 and is_json:
                nested_prefix = f'{field_source}[{field_config.index}].'
            elif is_repeated and field_config.index >= 0:
                nested_prefix = f'{field_source}[safe_offset({field_config.index})].'                
            elif field_config.pivot:
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

            dbt_field = Field()
            dbt_field.title = field_title
            dbt_field.required = field.required
            dbt_field.primaryKey = field.primaryKey
            dbt_field.pii = field.pii
            dbt_field.classification = field.classification
            dbt_field.description = field.description
            dbt_field.config = field_config.model_dump(exclude_none=True, exclude_defaults=True)

            dbt_field.type = field.type

            field_type_converted = map_type_to_bigquery(field)
            if field.type and field_type_converted != 'variant':
                field_type = field_type_converted.upper() if field_type_converted else field.type.upper()
            else:
                field_type = 'STRING'

            field_config.bigqueryType = field_type

            # Type conversion.
            if is_json and not is_calculated and field_config.pivotKeyFilter is None and field_category in ['data', 'metadata']:
                cast_field_source = _json_to_scalar_bigquery(field_source, field_type, truncate_timestamp=truncate_timestamp)
            elif not is_json and (is_calculated or data_type_overwrite) and field_config.pivotKeyFilter is None and field_category in ['data', 'metadata']:
                cast_field_source = f"cast( {field_source:<28}{' as ' + field_type.lower() + ' )':<20}"
            elif pivot_source_field is not None and field_config.pivotKeyFilter is not None and field_category in ['data', 'metadata']:
                field_source_prefix = '__unnested.'
                if is_calculated:
                    field_source_prefix = ''

                if field_config.calculation:
                    pivot_field = f"{field_source_prefix}{field_config.calculation}"
                else:
                    pivot_field = f"{field_source_prefix}{quote_name(field_config.pivotValueField)}"
 
                json_unnest_prefix = ''
                sql_column_filter = f"(__unnested.{quote_name(field_config.pivotKeyField)}) = {field_config.pivotKeyFilter}"
                
                if is_json:
                    json_unnest_prefix = 'json_extract_array'
                    pivot_filter_value = field_config.pivotKeyFilter.replace("'", "")
                    sql_column_filter = f"LAX_STRING(__unnested.{quote_name(field_config.pivotKeyField)}) = '{pivot_filter_value}'"
                    
                    pivot_field = _json_to_scalar_bigquery(pivot_field, field_type, truncate_timestamp = truncate_timestamp)

                cast_field_source = f"( select {pivot_field} from unnest({json_unnest_prefix}({pivot_source_field})) as __unnested where {sql_column_filter} )"
            else:
                cast_field_source = field_source

            field_source = cast_field_source 
            field_config.extended.source = field_source
            
            dbt_field.tags = field.tags

            # Data masking rules to be applied to the field using DB views.
            field_secured_value = security_masks.get(field_config.security)
            if field_secured_value:
                field_secured_value = field_secured_value.replace('{}', field_title)

            field_config.extended.secured_value = field_secured_value
            field_config.extended.category = field_category
            
            dbt_field.config = field_config.model_dump(exclude_none=True, exclude_defaults=True)

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
