import os
from typing import Any, Dict
from datacontract.model.data_contract_specification import Server


def get_pyodbc_connection_string(server: Server) -> str:
    """Generate a connection string for pyodbc using server configuration.
    
    Args:
        server: Server configuration object
        
    Returns:
        A connection string suitable for pyodbc.connect()
    """
    # Get credentials from environment variables
    username = os.getenv("DATACONTRACT_SQLSERVER_USERNAME", "")
    password = os.getenv("DATACONTRACT_SQLSERVER_PASSWORD", "")
    
    # Get connection options
    driver = server.driver or "ODBC Driver 18 for SQL Server"
    trusted_connection = os.getenv("DATACONTRACT_SQLSERVER_TRUSTED_CONNECTION", "no").lower() == "yes"
    trust_server_certificate = os.getenv("DATACONTRACT_SQLSERVER_TRUST_SERVER_CERTIFICATE", "no").lower() == "yes"
    encrypt = os.getenv("DATACONTRACT_SQLSERVER_ENCRYPTED_CONNECTION", "yes").lower() == "yes"
    
    # Build the connection string
    conn_parts = [
        f"DRIVER={{{driver}}};",
        f"SERVER={server.host},{server.port or 1433};",
        f"DATABASE={server.database};"
    ]
    
    # Add authentication method
    if trusted_connection:
        conn_parts.append("Trusted_Connection=yes;")
    else:
        conn_parts.append(f"UID={username};PWD={password};")
    
    # Add encryption settings
    if encrypt:
        conn_parts.append("Encrypt=yes;")
    else:
        conn_parts.append("Encrypt=no;")
        
    if trust_server_certificate:
        conn_parts.append("TrustServerCertificate=yes;")
    
    # Join all parts into a single string
    connection_string = "".join(conn_parts)
    
    return connection_string


def get_table_metadata(server: Server, table_name: str) -> Dict[str, Any]:
    """Retrieve metadata for a specific table from a SQL Server database.
    
    Args:
        server: Server configuration object
        table_name: Name of the table to get metadata for
        
    Returns:
        Dictionary containing table metadata
    """
    import pyodbc
    
    # Get connection string
    conn_str = get_pyodbc_connection_string(server)
    
    try:
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get the schema from server or default to 'dbo'
        schema = server.schema_ or 'dbo'

        # SQL script to get column information
        sql = """
        SELECT 
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            CAST(CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS is_primary_key,
            ep.value AS column_description
        FROM 
            sys.columns c
        INNER JOIN 
            sys.types t ON c.user_type_id = t.user_type_id
        INNER JOIN 
            sys.tables tb ON c.object_id = tb.object_id
        INNER JOIN 
            sys.schemas s ON tb.schema_id = s.schema_id
        LEFT JOIN 
            (SELECT 
                ic.column_id, 
                ic.object_id
             FROM 
                sys.index_columns ic
             INNER JOIN 
                sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
             WHERE 
                i.is_primary_key = 1
            ) pk ON c.column_id = pk.column_id AND c.object_id = pk.object_id
        LEFT JOIN 
            sys.extended_properties ep ON c.object_id = ep.major_id 
                                      AND c.column_id = ep.minor_id
                                      AND ep.name = 'MS_Description'
        WHERE 
            s.name = ? AND tb.name = ?
        ORDER BY 
            c.column_id
        """
        
        cursor.execute(sql, schema, table_name)
        columns = cursor.fetchall()
        
        if not columns:
            raise ValueError(f"Table '{schema}.{table_name}' not found in the database")
        
        # Get table description
        sql_table_desc = """
        SELECT 
            ep.value AS table_description
        FROM 
            sys.tables t
        INNER JOIN 
            sys.schemas s ON t.schema_id = s.schema_id
        LEFT JOIN 
            sys.extended_properties ep ON t.object_id = ep.major_id 
                                      AND ep.minor_id = 0
                                      AND ep.name = 'MS_Description'
        WHERE 
            s.name = ? AND t.name = ?
        """
        
        cursor.execute(sql_table_desc, schema, table_name)
        table_desc_row = cursor.fetchone()
        table_description = table_desc_row[0] if table_desc_row else None
        
        # Process column data
        processed_columns = []
        for column in columns:
            col_name, data_type, max_length, precision, scale, is_nullable, is_identity, is_pk, description = column
            
            # Format the complete data type with size if applicable
            if data_type in ('char', 'varchar', 'nchar', 'nvarchar', 'binary', 'varbinary'):
                if max_length == -1:
                    full_data_type = f"{data_type}(MAX)"
                else:
                    if data_type.startswith('n'):  # Unicode types store 2 bytes per char
                        max_length = max_length // 2
                    full_data_type = f"{data_type}({max_length})"
            elif data_type in ('decimal', 'numeric'):
                full_data_type = f"{data_type}({precision}, {scale})"
            else:
                full_data_type = data_type
            
            processed_columns.append({
                "name": col_name,
                "data_type": full_data_type,
                "base_type": data_type,
                "max_length": max_length if data_type in ('char', 'varchar', 'nchar', 'nvarchar') else None,
                "precision": precision if data_type in ('decimal', 'numeric') else None,
                "scale": scale if data_type in ('decimal', 'numeric') else None,
                "is_identity": bool(is_identity),
                "is_required": not bool(is_nullable),
                "is_primary_key": bool(is_pk),
                "description": description
            })
        
        # Close connection
        conn.close()
        
        # Return metadata dictionary
        return {
            "table_name": table_name,
            "schema": schema,
            "server_type": "sqlserver",
            "description": table_description,
            "columns": processed_columns
        }
            
    except Exception as e:
        raise RuntimeError(f"Error retrieving SQL Server table metadata: {str(e)}")
