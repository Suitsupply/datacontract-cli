import re
import json
from dbtgenerate.source.configs.config_yml import ConfigContract
from dbtgenerate.source.configs.config_security_yml import ConfigSecurity

"""Data Contract Processing Module.

This module provides classes for processing and managing data contracts in dbt projects.
It handles contract fields, data type transformations, security configurations, and
model specifications. The module supports JSON field processing, field aliasing,
and security masks.

Classes:
    ContractField: Handles individual field specifications and transformations
    Contract: Manages complete data contracts with provider and security configurations

Example:
    contract = Contract(
        contract_yaml="...",
        provider_yaml="...",
        security_yaml="..."
    )
"""

class ContractField:
    """Process and transform individual contract fields.

    This class handles field name quoting, cleaning, and JSON field casting.
    It manages field metadata, security configurations, and type transformations.

    Methods:
        quote_name: Properly quote field names for BigQuery
        clean_name: Clean field names for dbt compatibility
        cast_json_field: Apply proper casting for JSON fields
    """

    def quote_name(_field_name):

        pattern = '^[a-zA-Z0-9_.]+$'
        pattern_wq = '^`.+`$'

        if not _field_name:
            return _field_name

        if bool(re.match(pattern, _field_name)):
            return _field_name

        if bool(re.match(pattern_wq, _field_name)):
            return _field_name

        return f"`{_field_name}`"


    def clean_name(_field_name):
        
        if not _field_name:
            return _field_name

        pattern = '[^a-zA-Z0-9_]'

        return re.sub(pattern, '_', _field_name.strip().replace('`',''))        


    def cast_json_field(field_source, field_type, truncate_timestamp):
        #JSON type conversion functions in BigQuery use BOOL instead of BOOLEAN
        if field_type == 'BOOLEAN':
            field_type = 'BOOL'
        elif field_type == 'NUMERIC':
            field_type = 'FLOAT64'
        elif field_type == 'INTEGER':
            field_type = 'INT64' 

        cast_start = ""
        cast_end = ""  
        safe_cast_prefix = 'lax_'

        # Mapping between target data type and BigQuery function for JSON scalar conversion.
        bq_json_types = ["BOOL","INT64","FLOAT64","STRING"]
        
        # Convert all DATES
        if field_type in ("DATE", "DATETIME", "TIMESTAMP") and truncate_timestamp:
            cast_start = f"{field_type.lower()}(nullif(left({safe_cast_prefix}string("
            cast_end = "), 26), ''))"
        elif field_type in ("DATE", "DATETIME", "TIMESTAMP"):
            cast_start = f"{field_type.lower()}(nullif({safe_cast_prefix}string("
            cast_end = "), ''))"

        # Convert the field to one of the target formats, otherwise, return the field without data conversion (for example for JSON target format).
        elif field_type in bq_json_types:
            cast_start = f"{safe_cast_prefix}{field_type.lower()}("
            cast_end = ")"

        return f"{cast_start}{field_source}{cast_end}"
    
    
    def __init__(self, field_category, field_source, field_alias, field_description, field_type, field_mode, field_meta = {}):

        self.field_category = field_category
        self.field_alias = field_alias.lower()
        self.field_type = field_type
        self.field_description = field_description
        self.field_mode = field_mode
        self.field_meta = field_meta

        self.is_calculated = field_meta.get('is_calculated', False)
        self.is_json = field_meta.get('is_json', False)
        self.contains_pii = field_meta.get('contains_pii', False)
        self.security = field_meta.get('security', None)
        self.secured_value = field_meta.get('secured_value', None)
        self.truncate_timestamp = field_meta.get('truncate_timestamp', False)
        self.data_type_overwrite = field_meta.get('data_type_overwrite', False)
        self.pivot_conditon = field_meta.get('pivot_conditon', None)
        self.pivot_field_source = field_meta.get('pivot_field_source', False)
        self.pivot_base_field = field_meta.get('pivot_base_field', None)

        if field_alias == 'duration':
            pass
        
        if self.secured_value:
            self.secured_value = self.secured_value.replace('{}', self.field_alias)

        # Type conversion.
        if self.is_json and not self.is_calculated and self.pivot_conditon is None and field_category in ['data', 'metadata']:
            cast_field_source = ContractField.cast_json_field(field_source, field_type, truncate_timestamp=self.truncate_timestamp)
        elif not self.is_json and (self.is_calculated or self.data_type_overwrite) and self.pivot_conditon is None and field_category in ['data', 'metadata']:
            cast_field_source = f"cast( {field_source:<28}{' as ' + field_type.lower() + ' )':<20}"
        elif self.pivot_field_source is not None and self.pivot_conditon is not None and field_category in ['data', 'metadata']:
            field_source_prefix = '__unnested.'
            if self.is_calculated:
                field_source_prefix = ''

            pivot_field = f"{field_source_prefix}{field_source}"
            json_unnest_prefix = ''
            base_column_filter = f"(__unnested.{self.pivot_base_field}) = {self.pivot_conditon}"
            if self.is_json:
                json_unnest_prefix = 'json_extract_array'
                base_column_filter = f"LAX_STRING(__unnested.{self.pivot_base_field}) = '{self.pivot_conditon}'"
                pivot_field = ContractField.cast_json_field(pivot_field, field_type, truncate_timestamp=self.truncate_timestamp)

            cast_field_source = f"( select {pivot_field} from unnest({json_unnest_prefix}({self.pivot_field_source})) as __unnested where {base_column_filter} )"
        else:
            cast_field_source = field_source

        self.field_source = cast_field_source 


class Contract:
    """Process and manage complete data contracts.

    This class handles the complete data contract lifecycle including:
    - Contract initialization from YAML configurations
    - Security settings and masks
    - Field generation and transformation
    - Model configuration and metadata

    Args:
        contract_yaml: YAML string containing contract specifications
        provider_yaml: YAML string containing provider specifications
        security_yaml: Optional YAML string with security configurations

    Attributes:
        column_data: List of ContractField objects
        primary_keys: List of primary key field names
        filters: List of filter configurations
        entity: Name of the entity
        description: Entity description
        identifier: Table identifier
        product: Product identifier
        entity_label: Combined product and entity label
        raw_model: Name of the raw model
        source_model: Name of the source model
        staging_model: Name of the staging model
        ownership_team: Team owning the model
        ownership_email: Team email contact
        project: Project ID
        loader: Data loader type
        dataset: Dataset name
        refresh_mode: Refresh mode (LIVE/BATCH)
        refresh_frequency: Refresh frequency
        recency_validation: Field for recency checks
        order_by: Field for ordering
        snapshot_status: Snapshot configuration
        deduplication: Deduplication status
        source_mode: Source mode configuration
        cluster_by: Clustering fields
        recency_threshold: Threshold for recency checks
        has_deleted_flag: Flag for soft deletes
        has_primary_key: Flag for primary key presence
        data_type_overwrite: Type overwrite configuration
        num_of_nested_joins: Count of nested joins
        source_schema_root_field: Root field for nested schemas
        sql_join: JOIN clauses
        sql_where: WHERE clauses
        security: Security configuration
        security_filters: Security filter clauses
        security_ctes: Security CTEs
        security_masks: Security mask configurations
        config_meta: Model metadata
        config_tags: Model tags
        config_labels: Model labels

    Methods:
        reset_attributes: Reset all attributes to default values
        set_security: Configure security settings
        set_value: Set attribute values with fallbacks
        generate_source_fields: Generate field configurations
    """

    def reset_attributes(self):

        self.column_data = []
        self.primary_keys = []
        self.filters = []

        self.entity = ''
        self.description = ''
        self.identifier = ''
        self.product = ''
        self.entity_label = ''

        self.raw_model = ''
        self.source_model = ''
        self.staging_model = ''

        self.ownership_team = ''
        self.ownership_email = ''    
        
        self.project = ''
        self.loader = ''
        self.dataset = ''    
        
        self.refresh_mode = ''
        self.refresh_frequency = ''
        self.recency_validation = ''
        self.order_by = ''
        self.snapshot_status = ''
        self.deduplication = ''
        self.source_mode = ''
        self.cluster_by = ''
        self.recency_threshold = 0

        self.has_deleted_flag = False
        self.has_primary_key = False
        self.data_type_overwrite = ''
        self.num_of_nested_joins = 0
        
        self.source_schema_root_field = None
        self.sql_join = ''
        self.sql_where = ''
        
        self.security = ''    
        self.security_filters = ''
        self.security_ctes = ''
        self.security_masks = {}

        self.config_meta = {}
        self.config_tags = []
        self.config_labels = {}


    def __init__(self, contract_yaml, provider_yaml, security_yaml = None):

        # reset class attributes
        self.reset_attributes()

        # initialize class attributes
        config_contract = ConfigContract.from_string(contract_yaml)
        config_provider = ConfigContract.from_string(provider_yaml)

        self.contract_yaml = contract_yaml
        self.provider_yaml = provider_yaml
        
        # set entity name
        self.entity = config_contract.entity

        # set description
        self.description = config_contract.description.strip()
        # set loader
        self.loader = config_provider.entity

        # set general attributes
        self.set_value('product', config_contract.product, config_provider.product)       
        self.set_value('identifier', config_contract.identifier, config_provider.identifier, config_contract.entity)
        self.set_value('project',config_contract.project, config_provider.project)
        self.set_value('dataset',config_contract.dataset, config_provider.dataset, config_provider.entity)
        self.set_value('ownership_team',config_contract.ownership.team, config_provider.ownership.team)
        self.set_value('ownership_email',config_contract.ownership.email, config_provider.ownership.email)

        self.set_value('recency_validation', config_contract.source_schema.recency_validation, config_provider.source_schema.recency_validation, '_loaded_at')
        self.set_value('order_by', config_contract.source_schema.order_by, config_provider.source_schema.order_by, '_loaded_at')

        self.set_value('refresh_frequency', config_contract.refresh_policy.frequency.value, config_provider.refresh_policy.frequency.value, 'DAILY')
        self.set_value('snapshot_status', config_contract.refresh_policy.snapshot_status.value, config_provider.refresh_policy.snapshot_status.value)
        self.set_value('data_type_overwrite', config_contract.refresh_policy.data_type_overwrite.value, config_provider.refresh_policy.data_type_overwrite.value)
        self.set_value('deduplication', config_contract.refresh_policy.deduplication.value, config_provider.refresh_policy.deduplication.value)
        self.set_value('refresh_mode', config_contract.refresh_policy.refresh_mode.value, config_provider.refresh_policy.refresh_mode.value, 'LIVE')
        self.set_value('cluster_by', str(config_contract.refresh_policy.cluster_by), str(config_provider.refresh_policy.cluster_by), "[]")
        self.set_value('recency_threshold', config_contract.refresh_policy.recency_threshold, config_provider.refresh_policy.recency_threshold, 0)
        self.set_value('partition_expiration_days', config_contract.refresh_policy.partition_expiration_days, config_provider.refresh_policy.partition_expiration_days, -1)

        self.set_value('filters', config_contract.source_schema.filters, config_provider.source_schema.filters, [])        
        self.set_value('security',config_contract.security, config_provider.security, 'table_access_all')

        # set config attributes
        self.config_meta = {'owner': self.ownership_team,'owner_email': self.ownership_email}
        if self.security:
            self.config_meta['security'] = self.security

        self.config_tags = [self.refresh_frequency, config_provider.entity, config_contract.product]
        
        self.entity_label = f"{self.product}__{self.entity}"
        self.raw_model = f'raw__{self.entity_label}'
        self.source_model = f'source__{self.entity_label}'
        self.staging_model = f'staging__{self.entity_label}'
        
        self.config_labels = {}

        self.config_labels['source_table'] = self.identifier.lower().replace('*', '-tableset')
        if (len(self.identifier) > 63):
            self.config_labels['source_table'] = f"{self.identifier.lower()[:53]}-truncated"
        
        self.config_labels['source_dataset'] = self.dataset.lower()
        
        self.config_labels['snapshot_enabled'] = 'no'
        if self.snapshot_status == "ENABLED":
            self.config_labels['snapshot_enabled'] = 'yes'

        self.config_labels['contains_pii'] = 'no'
        if (config_contract.contains_pii):
            self.config_labels['contains_pii']= 'yes'
            self.config_tags.append('contains_pii')

        # set "has primary key" flag
        if config_contract.source_schema.primary_keys is not None:
            self.has_primary_key = len(config_contract.source_schema.primary_keys) > 0
            for key in config_contract.source_schema.primary_keys:
                self.primary_keys.append(key.lower())

        # set "has deleted flag"
        for metadata_field in config_provider.source_schema.fields:
            if metadata_field.alias == '_deleted':
                self.has_deleted_flag = True
                break            

        # generate list of SQL fields based on the provider metadata fields
        self.generate_source_fields(
            field_category='metadata',
            fields=config_provider.source_schema.fields
            )

        # generate list of filters
        for filter in self.filters:
            if self.sql_where == '':
                sql_where_connector = '\n    where'
            else:
                sql_where_connector = '\n    and'

            env_filter = ''
            if filter.dev_only:
                env_filter = " {{' or true' if target.name in ['acc','prd'] else ''}}"

            self.sql_where += f"{sql_where_connector} ({filter.field} {filter.operator} {filter.value}{env_filter})"       

        # set security settings
        if security_yaml:
            self.set_security(security_yaml=security_yaml)

        # generate source fields
        self.source_schema_root_field = config_provider.source_schema_root_field
        if config_contract.source_schema_root_field:
            self.source_schema_root_field = config_contract.source_schema_root_field            

        if self.source_schema_root_field:
            self.source_schema_root_field.fields = config_contract.source_schema.fields
            source_fields = [self.source_schema_root_field]
        else:
            source_fields = config_contract.source_schema.fields

        self.generate_source_fields(
            field_category='data',
            fields=source_fields)
        
        primary_key = "''"
        if self.has_primary_key:
            primary_key = f"{{{{ dbt_utils.generate_surrogate_key({str(config_contract.source_schema.primary_keys)}) }}}}"

        contract_field = ContractField(
              field_category = "primary_key"
            , field_source = primary_key
            , field_alias = '_primary_key'
            , field_description = f"Primary key."
            , field_type = 'STRING'
            , field_mode = "REQUIRED"
            , field_meta = {"is_calculated": True}
        )
        
        self.column_data.append(contract_field)


    def set_security(self, security_yaml):
        
        self.security_masks = {}
        if not security_yaml:
            return

        config_security = ConfigSecurity.from_string(security_yaml)

        # generate security cte
        self.security_ctes = ''
        for cte in config_security.ctes:
            if self.security_ctes:
                self.security_ctes += f'\n    ,{cte.name} as ({cte.query})'
            else:
                self.security_ctes += f'\n    {cte.name} as ({cte.query})'   

        # generate security filters
        for filter in config_security.security:
            if filter.name == self.security:
                if self.security_filters == '':
                    sql_where_connector = '\n    where'
                else:
                    sql_where_connector = '\n    and'
                self.security_filters += f"{sql_where_connector} ({filter.query})"

        # generate security_masks
        for mask in config_security.column_security:
            self.security_masks[mask.name] = mask.query
        
        if self.security_filters == '':
            self.security_filters = '\n    where false'         


    def set_value(self, attribute_name, first_value, second_value, default_value = None):

        attribute_value = None
        if first_value is not None and str(first_value).lower() != 'null' and str(first_value) != '[]':
            attribute_value = first_value
        elif second_value is not None and str(second_value).lower() != 'null' and str(second_value) != '[]':
            attribute_value = second_value  
        else: 
            attribute_value = default_value

        setattr(self, attribute_name, attribute_value)


    def generate_source_fields(self, field_category, fields, column_prefix = "", is_parent_json = False, pivot_field_source = None):

        for field in fields:

            field_source = f"{column_prefix}{ContractField.quote_name(field.name)}"
            if field.calculated:
                field_source = field.name
                
            field_alias = ContractField.clean_name(field.name)
            if field.alias is not None:
                field_alias = ContractField.clean_name(field.alias)

            is_json = False
            if is_parent_json or field.type == 'JSON':
                is_json = True

            if field.type.name in ('JSON', 'RECORD') and len(field.fields) > 0:

                if not field.repeated:
                    nested_prefix = field_source + '.'
                elif field.repeated and field.index >= 0 and is_json:
                    nested_prefix = f'{field_source}[{field.index}].'
                elif field.repeated and field.index >= 0:
                    nested_prefix = f'{field_source}[safe_offset({field.index})].'                
                elif field.repeated and field.pivot:
                    nested_prefix = ''
                    pivot_field_source = field_source
                else:
                    self.num_of_nested_joins += 1
                    
                    nested_alias = f'nested_{field_alias}'
                    
                    nested_prefix = f"{nested_alias}."
                    json_unnest_prefix = ''
                    if is_json:
                        json_unnest_prefix = 'json_extract_array'
                    self.sql_join += f"left join unnest({json_unnest_prefix}({field_source})) as {nested_alias}\n"

                # recursive execution of the function to extract nested attributes
                self.generate_source_fields(
                      field_category = field_category
                    , fields=field.fields
                    , column_prefix=nested_prefix
                    , is_parent_json=is_json
                    , pivot_field_source=pivot_field_source)
                
            else:
                
                field_meta = {}
                if hasattr(field, 'meta'):
                    field_meta = field.meta

                field_meta.update({
                    "is_json": is_json,
                    "is_calculated": field.calculated,
                    "pivot_field_source": pivot_field_source,
                    "pivot_conditon": field.condition,
                    "pivot_base_field": ContractField.quote_name(field.base_field),
                    "contains_pii": field.contains_pii,
                    "data_type_overwrite": self.data_type_overwrite == 'ENABLED',
                    "truncate_timestamp": self.loader == 'cosmosdb',
                    "security": field.security,
                    "secured_value": self.security_masks.get(field.security)
                })

                contract_field = ContractField(
                      field_category = field_category
                    , field_source = field_source
                    , field_alias = field_alias
                    , field_description = field.description
                    , field_type = field.type.name
                    , field_mode = field.mode
                    , field_meta = field_meta)

                self.column_data.append(contract_field)


class Object:
    def toJSON(self):
        return json.dumps(self,default=vars)
