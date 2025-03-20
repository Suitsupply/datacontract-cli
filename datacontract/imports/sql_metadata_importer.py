import logging
from typing import Dict, Any

from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model, Server
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum
from datacontract.export.sql_type_converter import get_physical_type_key
# Import existing type conversion functions
from datacontract.imports.sql_importer import map_type_from_sql
from datacontract.imports.bigquery_importer import map_type_from_bigquery


class SqlMetadataImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        return import_sql_metadata(data_contract_specification, self.import_format, source, import_args)


def import_sql_metadata(
    data_contract_specification: DataContractSpecification,
    format: str,
    table_metadata: Dict[str, Any],
    import_args: dict = None
) -> DataContractSpecification:

    # Get required fields from metadata
    table_name = table_metadata.get("table_name")
    columns = table_metadata.get("columns", [])
    server_type = table_metadata.get("server_type")
    
    # Validate required fields
    if not table_name:
        raise DataContractException(
            type="import",
            name="Missing table name",
            reason="Table name is required in metadata",
            engine="datacontract",
            result=ResultEnum.error,
        )
    
    if not columns:
        raise DataContractException(
            type="import",
            name="Missing columns",
            reason=f"No columns found for table '{table_name}'",
            engine="datacontract",
            result=ResultEnum.error,
        )
    
    # Update server information if provided
    if server_type and data_contract_specification.servers is not None:
        if server_type not in data_contract_specification.servers:
            data_contract_specification.servers[server_type] = Server(type=server_type)
    
    # Initialize models if needed
    if data_contract_specification.models is None:
        data_contract_specification.models = {}
    
    # Create fields dictionary from columns
    fields = {}
    for column in columns:
        col_name = column.get("name")
        if not col_name:
            logging.warning("Column missing name, skipping")
            continue
        
        field = Field()
        
        # Set field properties
        field.description = column.get("description")
        field.maxLength = column.get("max_length")
        field.precision = column.get("precision")
        field.scale = column.get("scale")
        field.primaryKey = column.get("is_primary_key")
        field.required = column.get("is_required")
        
        # Get SQL type
        sql_type = column.get("data_type")
        
        # Store original SQL type in config
        physical_type_key = get_physical_type_key(server_type)
        field.config = {
            physical_type_key: sql_type,
        }
        
        # Map SQL type to data contract type based on server type
        if server_type == "bigquery":
            field.type = map_type_from_bigquery(column.get("base_type", ""))
        else:
            field.type = map_type_from_sql(sql_type)
        
        fields[col_name] = field
    
    # Determine table type (table, view, etc.)
    table_type = "table"  # Default
    
    # For BigQuery, handle special table types
    if server_type == "bigquery" and "table_type" in table_metadata:
        from datacontract.imports.bigquery_importer import map_bigquery_type
        bigquery_table_type = table_metadata.get("table_type")
        table_type = map_bigquery_type(bigquery_table_type)
    
    # Add the table model to the data contract
    data_contract_specification.models[table_name] = Model(
        type=table_type,
        fields=fields,
        description=table_metadata.get("description"),
    )
    
    # For BigQuery, handle friendly name as title
    if server_type == "bigquery" and "friendly_name" in table_metadata:
        friendly_name = table_metadata.get("friendly_name")
        if friendly_name:
            data_contract_specification.models[table_name].title = friendly_name
    
    return data_contract_specification
