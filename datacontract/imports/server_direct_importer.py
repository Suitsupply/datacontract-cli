import importlib
import logging
import re
from typing import Dict, Callable, Any

from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Field, Model, Server
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum
from datacontract.export.sql_type_converter import convert_to_sql_type

# List of server types with get_table_metadata implementation
SUPPORTED_FORMATS = ["sqlserver", "postgres", "bigquery"]


def convert_to_snake_case(camel_input):
    """
    Convert camel case or snake case identifiers to standardized field names.
    
    Args:
        camel_input: The input field name to convert
        
    Returns:
        Standardized field name in snake_case
    """
    # Split by underscores first
    parts = camel_input.split('_')
    
    # Process each part: use regex only if it's not fully uppercase
    converted_parts = []
    for part in parts:
        if part.isupper():
            converted_parts.append(part.lower())  # Convert fully uppercase words to lowercase directly
        else:
            # Apply regex for camel case conversion
            words = re.findall(r'[A-Z]?[a-z]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+', part)
            converted_parts.append('_'.join(map(str.lower, words)))
    
    # Join parts back with underscores
    return '_'.join(converted_parts)


def get_physical_type_key(server_type: str) -> str:
    """
    Get the physical type key based on server type.
    
    Args:
        server_type: Type of database server
        
    Returns:
        The configuration key for the physical type
    """
    type_map = {
        "sqlserver": "sqlserverType",
        "postgres": "postgresType",
        "bigquery": "bigqueryType",
        "snowflake": "snowflakeType",
        "redshift": "redshiftType",
        "oracle": "oracleType",
        "mysql": "mysqlType",
        "databricks": "databricksType",
        "trino": "trinoType",
    }
    return type_map.get(server_type, "physicalType")


def import_sql_metadata(
    data_contract_specification: DataContractSpecification,
    format: str,
    table_metadata: Dict[str, Any],
    import_args: dict = None
) -> DataContractSpecification:
    """
    Import SQL table definition from metadata dictionary.
    
    Args:
        data_contract_specification: The data contract to update
        format: SQL format (e.g., "sql", "tsql")
        table_metadata: Dictionary containing table metadata
        import_args: Additional import arguments
        
    Returns:
        Updated data contract specification
    """
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
    
    # Set contract ID to table name (lowercase)
    data_contract_specification.id = convert_to_snake_case(table_name)
    data_contract_specification.info.title = table_name

    # Initialize models if needed
    if data_contract_specification.models is None:
        data_contract_specification.models = {}
    
    primaryKey = []

    # Create fields dictionary from columns
    fields = {}
    for column in columns:
        col_name = column.get("name")
        if not col_name:
            logging.warning("Column missing name, skipping")
            continue
        
        field = Field()
        
        # Set field properties
        field.title = convert_to_snake_case(col_name)
        field.description = column.get("description", '')
        field.maxLength = column.get("max_length")
        field.precision = column.get("precision")
        field.scale = column.get("scale")
        if field.primaryKey:
            field.primaryKey = column.get("is_primary_key")
            primaryKey.append(field.title)
        if field.required:
            field.required = column.get("is_required")
        
        # Get SQL type
        sql_type = column.get("data_type")
        
        # Map SQL type to data contract type based on server type
        if server_type == "bigquery":
            # Import on demand to avoid circular imports
            from datacontract.imports.bigquery_importer import map_type_from_bigquery
            field.type = map_type_from_bigquery(column.get("base_type", ""))
        else:
            # Use SQL importer's function for other databases
            from datacontract.imports.sql_importer import map_type_from_sql
            field.type = map_type_from_sql(sql_type)
        
        fields[col_name] = field

        # Store original SQL type in config using the server_type
        physical_type_key = get_physical_type_key(server_type)
        supported_sql_type = convert_to_sql_type(field, server_type)
        
        if not supported_sql_type or sql_type.lower() == 'json':
            field.config = {
                physical_type_key: sql_type.lower(),
            }
    
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
        primaryKey=primaryKey,
    )

    # For BigQuery, handle friendly name as title
    if server_type == "bigquery" and "friendly_name" in table_metadata:
        friendly_name = table_metadata.get("friendly_name")
        if friendly_name:
            data_contract_specification.models[table_name].title = friendly_name
    
    return data_contract_specification


# Function to dynamically import the module based on server type
def get_connection_module(server_type: str):
    """
    Dynamically import the appropriate connection module based on server type.
    
    Args:
        server_type: Type of database server
        
    Returns:
        The imported module
    """
    try:
        module_path = f"datacontract.engines.datacontract.connections.{server_type}"
        return importlib.import_module(module_path)
    except ImportError as e:
        raise DataContractException(
            type="import",
            name=f"Connection module not found",
            reason=f"No connection module found for server type: {server_type}. Error: {str(e)}",
            engine="datacontract",
            result=ResultEnum.error,
        )


# Function to get the table metadata function for a specific server type
def get_metadata_function(server_type: str) -> Callable:
    """
    Get the get_table_metadata function for a specific server type.
    
    Args:
        server_type: Type of database server
        
    Returns:
        The function to get table metadata
    """
    module = get_connection_module(server_type)
    
    if not hasattr(module, 'get_table_metadata'):
        raise DataContractException(
            type="import",
            name=f"Metadata function not implemented",
            reason=f"The get_table_metadata function is not implemented for server type: {server_type}",
            engine="datacontract",
            result=ResultEnum.error,
        )
    
    return getattr(module, 'get_table_metadata')


class ServerDirectImporter(Importer):    
    """
    Importer that fetches metadata directly from a database server.
    """
    
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        """
        Import table definition directly from a database server.
        
        Args:
            data_contract_specification: The data contract to update
            source: Source data contract file path
            import_args: Dictionary containing server and model information
            
        Returns:
            Updated data contract specification
        """
        # Check required arguments
        if 'server' not in import_args:
            raise DataContractException(
                type="import",
                name="Missing server argument",
                reason="The 'server' argument is required for server direct import",
                engine="datacontract",
                result=ResultEnum.error,
            )
        
        if 'model' not in import_args:
            raise DataContractException(
                type="import",
                name="Missing model argument",
                reason="The 'model' argument is required for server direct import",
                engine="datacontract",
                result=ResultEnum.error,
            )
        
        server_name = import_args['server']
        source_data_contract_specification = DataContractSpecification.from_file(source)
        server = source_data_contract_specification.servers.get(server_name)
        
        # Validate that the server exists
        if server is None:
            raise DataContractException(
                type="import",
                name="Server not found",
                reason=f"The server '{server_name}' does not exist in the data contract specification.",
                engine="datacontract",
                result=ResultEnum.error,
            )

        table_name = import_args['model']
        
        # Verify this is a Server object or create one from dictionary
        if isinstance(server, dict):
            server = Server(**server)
        
        # Verify server type is supported
        if server.type not in SUPPORTED_FORMATS:
            raise DataContractException(
                type="import",
                name="Invalid server type",
                reason=f"The server type '{server.type}' is not supported. Supported types: {', '.join(SUPPORTED_FORMATS)}",
                engine="datacontract",
                result=ResultEnum.error,
            )
        
        try:
            # Get the appropriate metadata function for this server type
            metadata_func = get_metadata_function(server.type)
            
            # Get the table metadata directly
            logging.info(f"Fetching metadata for table '{table_name}' from {server.type} server...")
            logging.info(f"Server details: Type={server.type}, Project={server.project}, Dataset={server.dataset}")
            
            metadata = metadata_func(server, table_name)
            logging.info(f"Successfully fetched metadata for table '{table_name}'")
            
            # Import the metadata directly
            logging.info(f"Importing table '{table_name}' definition...")
            updated_contract = import_sql_metadata(
                data_contract_specification, "sql", metadata, import_args
            )
            
            logging.info(f"Successfully imported table '{table_name}' from {server.type} server")
            return updated_contract
            
        except Exception as e:
            logging.error(f"Error fetching table metadata: {str(e)}")
            # Include more detailed server info in the error
            server_info = f"Type: {server.type}"
            if hasattr(server, 'project') and server.project:
                server_info += f", Project: {server.project}"
            if hasattr(server, 'dataset') and server.dataset:
                server_info += f", Dataset: {server.dataset}"
            
            raise DataContractException(
                type="import",
                name="Metadata fetch error",
                reason=f"Error fetching metadata for table '{table_name}' from server ({server_info}): {str(e)}",
                engine="datacontract",
                result=ResultEnum.error,
            )
