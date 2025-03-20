import importlib
import logging
from typing import Dict, Callable, Any

from datacontract.imports.importer import Importer
from datacontract.imports.sql_metadata_importer import import_sql_metadata
from datacontract.model.data_contract_specification import DataContractSpecification, Server
from datacontract.model.exceptions import DataContractException
from datacontract.model.run import ResultEnum

# List of server types with get_table_metadata implementation
SUPPORTED_FORMATS = ["sqlserver", "postgres", "bigquery"]

# Function to dynamically import the module based on server type
def get_connection_module(server_type: str):
    """Dynamically import the appropriate connection module based on server type."""
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
    """Get the get_table_metadata function for a specific server type."""
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
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        
        # Check required arguments
        if 'server' not in import_args:
            raise DataContractException(
                type="import",
                name="Missing server argument",
                reason="The 'server' argument is required for server direct import",
                engine="datacontract",
                result=ResultEnum.error,
            )
        
        if 'table' not in import_args:
            raise DataContractException(
                type="import",
                name="Missing table argument",
                reason="The 'table' argument is required for server direct import",
                engine="datacontract",
                result=ResultEnum.error,
            )
        
        server = import_args['server']
        table_name = import_args['table']
        
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
            raise DataContractException(
                type="import",
                name="Metadata fetch error",
                reason=f"Error fetching metadata for table '{table_name}': {str(e)}",
                engine="datacontract",
                result=ResultEnum.error,
            )
