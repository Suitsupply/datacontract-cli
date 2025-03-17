from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Definition, Field, Info, Model, Freshness
from datacontract.model.contract_alpha_specification import ConfigContract as ContractAlpha, Field as FieldAlpha, Filter as FilterAlpha
from datacontract.model.exceptions import DataContractException


class ContractAlphaImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        return import_contract(data_contract_specification, source)

def import_fields_alpha(
        fields_alpha: list[FieldAlpha],
        imported_definitions: dict[str, Definition] = {},
        primary_keys: list[str] = None,
        ):

    imported_fields = {}

    for field_alpha in fields_alpha:
        
        if not field_alpha.calculated:
            
            field = Field()
            config = {}
            config['dbt'] = {}

            field.title = field_alpha.name if field_alpha.alias is None else field_alpha.alias
            field.description = field_alpha.description
            field.required = field_alpha.mode == "REQUIRED"
            if field.title in primary_keys:
                field.primary_key = True

            field_alpha.type = field_alpha.type.value
            field.type = map_type_from_bigquery(field_alpha.type)
            
            if field_alpha.type in ("RECORD", "JSON"):

                if field_alpha.type == "JSON":
                    config["bigqueryType"] = "json"

                if field_alpha.repeated:
                    field.type = "array"
                    field.items = Field()

                    # Unpack single record in array
                    if field_alpha.index > 0:
                        config['dbt']['index'] = field_alpha.index

                    # Pivot array with calculated definitions
                    if field_alpha.pivot:
                        config['dbt']['enabled'] = False
                        config['dbt']['pivot'] = True

                        definition = Definition()
                        definition.title = field.title

                        definition.config = {}
                        if field_alpha.type == "JSON":
                            definition.config["bigqueryType"] = "json"                        
                        definition.config['dbt'] = {}
                        definition.config['dbt']['pivot'] = True

                        nested_fields = {}
                        for nested_field in field_alpha.fields:
                            nested_definition = Definition()
                            nested_definition.title = field_alpha.name if nested_field.alias is None else nested_field.alias
                            nested_definition.type = map_type_from_bigquery(nested_field.type)
                            
                            nested_definition.config = {}
                            nested_definition.config['dbt'] = {}
                            nested_definition.config['dbt']['base_value'] = nested_field.condition
                            nested_definition.config['dbt']['base_field'] = nested_field.base_field
                            if nested_field.security:
                                nested_definition.config['security'] = nested_field.security

                            nested_fields[nested_field.name] = nested_field

                        definition.fields = nested_fields
                        imported_definitions[field_alpha.name] = definition

                        for field in field_alpha.fields:
                            definition.fields = import_fields_alpha(field_alpha.fields, imported_definitions, primary_keys)
                            imported_definitions[field_alpha.name] = definition
                        
                        definition.fields = import_fields_alpha(field_alpha.fields, imported_definitions, primary_keys)
                        imported_definitions[field_alpha.name] = definition

                    field.items.fields = import_fields_alpha(field_alpha.fields, imported_definitions, primary_keys)
                else:
                    field.type = "record"
                    field.fields = import_fields_alpha(field_alpha.fields, imported_definitions, primary_keys)

            if field_alpha.security:
                config['security'] = field_alpha.security

            if config not in ({}, {'dbt': {}}):
                field.config = config
            imported_fields[field_alpha.name] = field
        
        # Add calculated fields
        else:
            definition = Definition()
            definition.title = field_alpha.name if field_alpha.alias is None else field_alpha.alias
            definition.type = map_type_from_bigquery(field_alpha.type)
            definition.config = {}
            definition.config['dbt'] = {}
            definition.config['dbt']['calculation'] = field_alpha.name
            if field_alpha.security:
                definition.config['security'] = field_alpha.security
            
            imported_definitions[field_alpha.name] = definition

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
    else:
        raise DataContractException(
            type="schema",
            result="failed",
            name="Map bigquery type to data contract type",
            reason=f"Unsupported type {bigquery_type_str} in bigquery json definition.",
            engine="datacontract",
        )


def import_contract(data_contract_specification: DataContractSpecification, source: str) -> DataContractSpecification:
    if data_contract_specification.models is None:
        data_contract_specification.models = {}

    schema_alpha = ContractAlpha.from_file(source)

    definitions = {}
    config = {}
    config['dbt'] = {}

    title = schema_alpha.entity if schema_alpha.identifier is None else schema_alpha.identifier
    description = schema_alpha.description
    primary_keys = schema_alpha.source_schema.primary_keys
    type = "table"

    fields = import_fields_alpha(schema_alpha.source_schema.fields, definitions, primary_keys)
    
    filters = []
    for filter in schema_alpha.source_schema.filters:
        
        schema_filter = {}
        schema_filter['field'] = filter.field
        schema_filter['value'] = filter.value
        schema_filter['operator'] = filter.operator
        schema_filter['dev_only'] = filter.dev_only
        filters.append(schema_filter)

    if filters:
        config['dbt']['filters'] = filters

    config['dbt']['data_type_overwrite'] = schema_alpha.refresh_policy.data_type_overwrite == 'ENABLED'
    config['dbt']['snapshot'] = schema_alpha.refresh_policy.snapshot_status == 'ENABLED'
    config['dbt']['deduplication'] = schema_alpha.refresh_policy.deduplication == 'ENABLED'
    config['dbt']['order_by'] = schema_alpha.source_schema.order_by
    config['dbt']['cluster_by'] = schema_alpha.refresh_policy.cluster_by
    config['dbt']['incremental'] = schema_alpha.refresh_policy.refresh_mode == 'INCREMENTAL'
    config['dbt']['security'] = schema_alpha.security

    freshness = Freshness()
    freshness.timestampField = f"{schema_alpha.refresh_policy.recency_threshold}d"
    freshness.timestampField = schema_alpha.source_schema.recency_validation

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
                config=config,
                primaryKey=primary_keys,
                fields=fields,
            )
        },
        freshness=freshness,
        definitions=definitions,
    )

    return data_contract_specification
