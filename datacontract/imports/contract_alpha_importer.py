from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Field, Info, Model, Freshness, Retention, ServiceLevel
from datacontract.model.contract_alpha_specification import ConfigContract as ContractAlpha, Field as FieldAlpha, Filter as FilterAlpha
from datacontract.model.exceptions import DataContractException


class ContractAlphaImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        return import_contract(data_contract_specification, source)

def import_fields_alpha(
        fields_alpha: list[FieldAlpha],
        imported_ephemerals: dict[str, Field] = {},
        primary_keys: list[str] = None,
        ):

    imported_fields = {}

    for field_alpha in fields_alpha:

        field = Field()
        config = {}
        field_title = field_alpha.alias if field_alpha.alias is not None else field_alpha.name
        
        if not field_alpha.calculated:
            
            if field_alpha.alias and field_alpha.alias != field_alpha.name:
                field.title = field_title
            field.description = field_alpha.description
            if field_alpha.mode == 'REQUIRED':
                field.required = True
            if field_title in primary_keys:
                field.primary_key = True

            field.type = map_type_from_bigquery(field_alpha.type)
            
            if field_alpha.type in ("RECORD", "JSON"):

                if field_alpha.type == "JSON":
                    config["bigqueryType"] = "json"
                config['dbt'] = {}

                if field_alpha.repeated:
                    field.type = "array"
                    field.items = Field()
                    field.items.type = "record"

                    field.items.fields = import_fields_alpha(field_alpha.fields, imported_ephemerals, primary_keys)

                    # Unpack single record in array
                    if field_alpha.index >= 0:
                        config['dbt']['index'] = field_alpha.index

                    # Pivot array with calculated fields (ephemerals)
                    if field_alpha.pivot:
                        config['dbt']['enabled'] = False
                        config['dbt']['pivot'] = True

                        ephemeral = Field()
                        ephemeral.title = field.title
                        ephemeral.type = "record"

                        ephemeral.config = {}
                        if field_alpha.type == "JSON":
                            ephemeral.config["bigqueryType"] = "json"                        
                        ephemeral.config['dbt'] = {}
                        ephemeral.config['dbt']['pivot'] = True

                        nested_ephemerals = {}
                        
                        for nested_field in field_alpha.fields:
                            nested_ephemeral = Field()
                            nested_field_title = field_alpha.name if nested_field.alias is None else nested_field.alias
                            nested_ephemeral.type = map_type_from_bigquery(nested_field.type)
                            
                            nested_ephemeral.config = {}
                            nested_ephemeral.config['dbt'] = {}
                            nested_ephemeral.config['dbt']['pivot_key_field'] = nested_field.name
                            nested_ephemeral.config['dbt']['pivot_key_filter'] = nested_field.condition
                            nested_ephemeral.config['dbt']['pivot_value_field'] = nested_field.base_field
                            if nested_field.security:
                                nested_ephemeral.config['security'] = nested_field.security

                            nested_ephemerals[nested_field_title] = nested_ephemeral

                        ephemeral.fields = nested_ephemerals
                        imported_ephemerals[field_alpha.name] = ephemeral

                else:
                    field.type = "record"
                    field.fields = import_fields_alpha(field_alpha.fields, imported_ephemerals, primary_keys)

            if field_alpha.security:
                config['security'] = field_alpha.security

            if 'dbt' in config.keys() and config['dbt'] == {}:
                config.pop('dbt')

            if config != {}:
                field.config = config

            imported_fields[field_alpha.name] = field
        
        # Add calculated fields
        else:
            if not field_alpha.alias:
                raise DataContractException(
                    type="schema",
                    result="failed",
                    name="Import calculated field",
                    reason=f"Calculated field {field_alpha.name} must have an alias.",
                    engine="datacontract",
                )

            calculation = field_alpha.name
            field_alpha.calculated = False
            field_alpha.name = field_alpha.alias
            
            ephemerals = import_fields_alpha([field_alpha], imported_ephemerals, primary_keys)
            ephemeral = ephemerals[field_alpha.name]

            if not ephemeral.config:
                ephemeral.config = {}
            if not ephemeral.config.get('dbt'):
                ephemeral.config['dbt'] = {}
            ephemeral.config['dbt']['calculation'] = calculation

            imported_ephemerals[field_alpha.alias] = ephemeral

    return imported_fields


def map_type_from_bigquery(bigquery_type_str: str):
    if bigquery_type_str == "STRING":
        return "string"
    elif bigquery_type_str == "BYTES":
        return "bytes"
    elif bigquery_type_str == "INTEGER":
        return "int"
    elif bigquery_type_str == "INT64":
        return "bigint"
    elif bigquery_type_str == "FLOAT":
        return "float"
    elif bigquery_type_str == "FLOAT64":
        return "double"
    elif bigquery_type_str == "BOOLEAN" or bigquery_type_str == "BOOL":
        return "boolean"
    elif bigquery_type_str == "TIMESTAMP":
        return "timestamp"
    elif bigquery_type_str == "DATE":
        return "date"
    elif bigquery_type_str == "TIME":
        return "timestamp_ntz"
    elif bigquery_type_str == "DATETIME":
        return "timestamp"
    elif bigquery_type_str == "NUMERIC":
        return "numeric"
    elif bigquery_type_str == "BIGNUMERIC":
        return "double"
    elif bigquery_type_str == "GEOGRAPHY":
        return "object"
    elif bigquery_type_str == "JSON":
        return "object"
    elif bigquery_type_str == "RECORD":
        return "object"
    else:
        raise DataContractException(
            type="schema",
            result="failed",
            name="Map bigquery type to data contract type",
            reason=f"Unsupported type {bigquery_type_str} in bigquery json ephemeral.",
            engine="datacontract",
        )


def import_contract(data_contract_specification: DataContractSpecification, source: str) -> DataContractSpecification:
    if data_contract_specification.models is None:
        data_contract_specification.models = {}

    schema_alpha = ContractAlpha.from_file(source)

    ephemerals = {}
    config = {}
    config['dbt'] = {}

    title = schema_alpha.entity if schema_alpha.identifier is None else schema_alpha.identifier
    description = schema_alpha.description
    primary_keys = schema_alpha.source_schema.primary_keys
    type = "table"

    fields = import_fields_alpha(schema_alpha.source_schema.fields, ephemerals, primary_keys)
    
    filters = []
    for filter in schema_alpha.source_schema.filters:
        
        schema_filter = {}
        schema_filter['field'] = filter.field
        schema_filter['value'] = filter.value
        schema_filter['operator'] = filter.operator
        if filter.dev_only:
            schema_filter['dev_only'] = filter.dev_only
        filters.append(schema_filter)

    if filters:
        config['dbt']['filters'] = filters

    if schema_alpha.refresh_policy.data_type_overwrite == 'ENABLED':
        config['dbt']['data_type_overwrite'] = True
    if schema_alpha.refresh_policy.snapshot_status == 'ENABLED':
        config['dbt']['snapshot'] = True
    if schema_alpha.refresh_policy.deduplication == 'ENABLED':
        config['dbt']['deduplication'] = True
    if schema_alpha.source_schema.order_by:
        config['dbt']['order_by'] = schema_alpha.source_schema.order_by
    if schema_alpha.refresh_policy.cluster_by:
        config['dbt']['cluster_by'] = schema_alpha.refresh_policy.cluster_by
    if schema_alpha.refresh_policy.refresh_mode == 'INCREMENTAL':
        config['dbt']['incremental'] = True
    if schema_alpha.security:
        config['dbt']['security'] = schema_alpha.security

    if config in ({}, {'dbt': {}}):
        config = None

    data_contract_specification = DataContractSpecification(
        id=f"{schema_alpha.product}__{schema_alpha.entity}",
        info=Info(
            title=schema_alpha.entity,
            version="1.0.0",
        ),
        models={
            title: Model(
                description=description,
                type=type,
                title=title,
                primaryKey=primary_keys,
                fields=fields,
                ephemerals=ephemerals,
            )
        },
        
        servicelevels=ServiceLevel(),
    )

    if config not in ({}, {'dbt': {}}):
        data_contract_specification.models[title].config = config

    if schema_alpha.refresh_policy.recency_threshold and schema_alpha.refresh_policy.recency_threshold > 0:
        freshness = Freshness()
        freshness.threshold = f"{schema_alpha.refresh_policy.recency_threshold}d"
        freshness.timestampField = schema_alpha.source_schema.recency_validation
        data_contract_specification.servicelevels.freshness = freshness

    if schema_alpha.refresh_policy.partition_expiration_days and schema_alpha.refresh_policy.partition_expiration_days > 0:
        retention = Retention()
        retention.period = f"{schema_alpha.refresh_policy.partition_expiration_days}d"
        retention.timestampField = schema_alpha.source_schema.recency_validation
        data_contract_specification.servicelevels.retention = retention

    if data_contract_specification.servicelevels == ServiceLevel():
        data_contract_specification.servicelevels = None

    return data_contract_specification
