from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Model, Field, Info, ServiceLevel, Freshness, Retention
from datacontract.model.contract_alpha_specification import ConfigContract as ContractAlpha, Field as FieldAlpha
from datacontract.model.exceptions import DataContractException

from datacontract.model.config_dbt import FieldConfigDBT, ModelConfigDBT, FilterDBT


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
        field_config = FieldConfigDBT()

        field_title = field_alpha.alias if field_alpha.alias is not None else field_alpha.name
        
        if not field_alpha.calculated:
            
            if field_alpha.alias and field_alpha.alias != field_alpha.name:
                field.title = field_title
            field.description = field_alpha.description
            if field_alpha.mode == 'REQUIRED':
                field.required = True
            if field_title in primary_keys:
                field.primaryKey = True

            field.type = map_type_from_bigquery(field_alpha.type)
            if field.type == "timestamp_ntz":
                field.format = "datetime"

            if field_alpha.source_type:
                for source_type_key, source_type_value in field_alpha.source_type.items():
                    setattr(field_config, source_type_key, source_type_value)

            if field_alpha.type in ("RECORD", "JSON"):

                if field_alpha.type == "JSON":
                    field_config.bigqueryType = "json"
            
                if field_alpha.repeated:
                    field.type = "array"
                    field.items = Field()
                    field.items.type = "record"

                    field.items.fields = import_fields_alpha(field_alpha.fields, imported_ephemerals, primary_keys)

                    # Unpack single record in array
                    if field_alpha.index >= 0:
                        field_config.index = field_alpha.index

                    # Pivot array with calculated fields (ephemerals)
                    if field_alpha.pivot:
                        field_config.enabled = False
                        field_config.pivot = True

                        if imported_ephemerals.get(field_alpha.name) is None:
                            ephemeral = Field()
                        else:
                            ephemeral = imported_ephemerals[field_alpha.name].model_copy()

                        ephemeral.title = field.title
                        ephemeral.type = "record"

                        ephemeral_config = FieldConfigDBT()
                        ephemeral_config.pivot = True                        

                        if field_alpha.type == "JSON":
                            ephemeral_config.bigqueryType = "json"
                        
                        for nested_field in field_alpha.fields:
                            nested_ephemeral = Field()
                            nested_field_title = field_alpha.name if nested_field.alias is None else nested_field.alias
                            nested_ephemeral.type = map_type_from_bigquery(nested_field.type)
                            
                            nested_ephemeral_config = FieldConfigDBT()                            
                            nested_ephemeral_config.pivotKeyField = nested_field.base_field
                            nested_ephemeral_config.pivotKeyFilter = nested_field.condition
                            nested_ephemeral_config.pivotValueField = nested_field.name
                            if nested_field.security:
                                nested_ephemeral_config.security = nested_field.security

                            nested_ephemeral.config = nested_ephemeral_config.model_dump(exclude_none=True, exclude_defaults=True)

                            ephemeral.fields[nested_field_title] = nested_ephemeral

                        if ephemeral_config != FieldConfigDBT():
                            ephemeral.config = ephemeral_config.model_dump(exclude_none=True, exclude_defaults=True)

                        imported_ephemerals[field_alpha.name] = ephemeral

                else:
                    field.type = "record"
                    field.fields = import_fields_alpha(field_alpha.fields, imported_ephemerals, primary_keys)

            if field_alpha.security:
                field_config.security = field_alpha.security

            if field_config != FieldConfigDBT():
                field.config = field_config.model_dump(exclude_none=True, exclude_defaults=True)
            
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
            
            ephemeral_config = FieldConfigDBT() if not ephemeral.config else FieldConfigDBT.model_validate(ephemeral.config)
            ephemeral_config.calculation = calculation
            ephemeral.config = ephemeral_config.model_dump(exclude_none=True, exclude_defaults=True)
                             
            imported_ephemerals[field_alpha.alias] = ephemeral

    return imported_fields


def map_type_from_bigquery(bigquery_type_str: str):
    if bigquery_type_str == "STRING":
        return "string"
    elif bigquery_type_str == "BYTES":
        return "bytes"
    elif bigquery_type_str == "INTEGER":
        return "int"
    elif bigquery_type_str == "BIGINT":
        return "bigint"
    elif bigquery_type_str == "INT64":
        return "bigint"
    elif bigquery_type_str == "BIGINT":
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
        return "timestamp_ntz"
    elif bigquery_type_str == "NUMERIC":
        return "numeric"
    elif bigquery_type_str == "BIGNUMERIC":
        return "double"
    elif bigquery_type_str == "GEOGRAPHY":
        return "object"
    elif bigquery_type_str == "JSON":
        return "record"
    elif bigquery_type_str == "RECORD":
        return "record"
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

    model_alpha = ContractAlpha.from_file(source)

    ephemerals = {}
    model_config = ModelConfigDBT()

    title = model_alpha.entity if model_alpha.identifier is None else model_alpha.identifier
    description = model_alpha.description
    primary_keys = model_alpha.source_schema.primary_keys
    type = "table"

    fields = import_fields_alpha(model_alpha.source_schema.fields, ephemerals, primary_keys)

    filters = []
    for filter in model_alpha.source_schema.filters:

        config_filter = FilterDBT()
        config_filter.field = filter.field
        config_filter.value = filter.value
        config_filter.operator = filter.operator
        if filter.dev_only:
            config_filter.dev_only = filter.dev_only
        filters.append(config_filter)

    if filters:
        model_config.filters = filters

    if model_alpha.refresh_policy.data_type_overwrite == 'ENABLED':
        model_config.typeOverwrite = True
    if model_alpha.refresh_policy.snapshot_status == 'ENABLED':
        model_config.snapshot = True
    if model_alpha.refresh_policy.deduplication == 'ENABLED':
        model_config.deduplicate = True
    if model_alpha.source_schema.order_by:
        model_config.orderBy = model_alpha.source_schema.order_by
    if model_alpha.refresh_policy.cluster_by:
        model_config.clusterBy = model_alpha.refresh_policy.cluster_by
    if model_alpha.refresh_policy.refresh_mode == 'INCREMENTAL':
        model_config.incremental = True
    if model_alpha.security:
        model_config.security = model_alpha.security
    if model_alpha.refresh_policy.partition_expiration_days:
        model_config.partitionExpirationDays = model_alpha.refresh_policy.partition_expiration_days
    if model_alpha.refresh_policy.recency_threshold:
        model_config.recencyThreshold = model_alpha.refresh_policy.recency_threshold
    if model_alpha.source_schema.recency_validation:
        model_config.recencyField = model_alpha.source_schema.recency_validation
    else:
        model_config.recencyField = '_loaded_at'    

    servicelevels=ServiceLevel()

    if model_alpha.refresh_policy.recency_threshold and model_alpha.refresh_policy.recency_threshold > 0:
        freshness = Freshness()
        freshness.threshold = f"{model_alpha.refresh_policy.recency_threshold}D"
        freshness.timestampField = model_alpha.source_schema.recency_validation
        servicelevels.freshness = freshness

    if model_alpha.refresh_policy.partition_expiration_days and model_alpha.refresh_policy.partition_expiration_days > 0:
        retention = Retention()
        retention.period = f"{model_alpha.refresh_policy.partition_expiration_days}D"
        retention.timestampField = model_alpha.source_schema.recency_validation
        servicelevels.retention = retention

    data_contract_specification = DataContractSpecification(
        dataContractSpecification="1.1.0",
        id=f"{model_alpha.entity}",
        info=Info(
            title=model_alpha.entity,
            version="1.0.0",
        ),
        models={
            title: Model(
                description=description,
                type=type,
                title=f"{model_alpha.entity}",
                primaryKey=primary_keys,
                fields=fields,
            )
        },
    )

    if servicelevels != ServiceLevel():
        data_contract_specification.servicelevels = servicelevels
    
    if ephemerals != {}:
        model_config.ephemerals = ephemerals

    if model_config != ModelConfigDBT():
        data_contract_specification.models[title].config = model_config.model_dump(exclude_none=True, exclude_defaults=True)

    return data_contract_specification
