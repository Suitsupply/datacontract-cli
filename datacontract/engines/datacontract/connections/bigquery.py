import os
from typing import Dict, Any, List
import logging

from datacontract.model.data_contract_specification import Server


def get_connection_config(server: Server) -> Dict[str, Any]:
    """Generate a connection configuration for BigQuery using server configuration.
    
    Args:
        server: Server configuration object
        
    Returns:
        A dictionary with connection configuration
    """
    # Get credentials from environment variables
    account_info = os.getenv("DATACONTRACT_BIGQUERY_ACCOUNT_INFO_JSON_PATH")
    if account_info is None:
        # Fall back to the default Google environment variable
        account_info = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    return {
        "project_id": server.project,
        "dataset": server.dataset,
        "account_info_json_path": account_info,
    }


def get_table_metadata(server: Server, table_name: str) -> Dict[str, Any]:
    """Retrieve metadata for a specific table from a BigQuery dataset.
    
    Args:
        server: Server configuration object
        table_name: Name of the table to get metadata for
        
    Returns:
        Dictionary containing table metadata
    """
    try:
        from google.cloud import bigquery
    except ImportError as e:
        raise RuntimeError(
            "BigQuery client library not found. Install with 'pip install google-cloud-bigquery'."
        )
    
    # Get connection details
    project_id = server.project
    dataset = server.dataset
    
    # Check if required configuration is present
    if not project_id:
        raise ValueError("Missing 'project' in server configuration")
    if not dataset:
        raise ValueError("Missing 'dataset' in server configuration")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Get table reference
        table_ref = f"{project_id}.{dataset}.{table_name}"
        
        # Get table metadata
        table = client.get_table(table_ref)
        
        # Process column data
        processed_columns = []
        for field in table.schema:
            # Create column metadata
            column_metadata = {
                "name": field.name,
                "data_type": field.field_type,
                "base_type": field.field_type,
                "description": field.description,
                "is_required": field.mode == "REQUIRED",
                "is_primary_key": False  # BigQuery doesn't have primary keys, but we include for compatibility
            }
            
            # Handle precision and scale for NUMERIC and BIGNUMERIC types
            if field.field_type in ("NUMERIC", "BIGNUMERIC"):
                column_metadata["precision"] = field.precision
                column_metadata["scale"] = field.scale
            
            # Handle max length for STRING type
            if field.field_type == "STRING" and hasattr(field, "max_length") and field.max_length is not None:
                column_metadata["max_length"] = field.max_length
            
            # Add to columns list
            processed_columns.append(column_metadata)
        
        # Return metadata dictionary
        return {
            "table_name": table_name,
            "schema": dataset,
            "server_type": "bigquery",
            "description": table.description,
            "friendly_name": table.friendly_name,
            "table_type": table.table_type,  # TABLE, VIEW, MATERIALIZED_VIEW, etc.
            "columns": processed_columns
        }
        
    except Exception as e:
        raise RuntimeError(f"Error retrieving BigQuery table metadata: {str(e)}")
