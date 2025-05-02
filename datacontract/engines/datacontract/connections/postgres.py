import os
from typing import Dict, Any
from datacontract.model.data_contract_specification import Server

def get_table_metadata(server: Server, table_name: str) -> Dict[str, Any]:
    """Retrieve metadata for a specific table from PostgreSQL database.
    
    Args:
        server: Server configuration object
        table_name: Name of the table to get metadata for
        
    Returns:
        Dictionary containing table metadata
    """
    import psycopg2
    
    # Get connection details
    host = server.host
    port = server.port or 5432
    dbname = server.database
    user = os.getenv("DATACONTRACT_POSTGRES_USERNAME", "")
    password = os.getenv("DATACONTRACT_POSTGRES_PASSWORD", "")
    schema = server.schema_ or 'public'
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host, 
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        
        # Check if the table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema, table_name))
        
        if not cursor.fetchone()[0]:
            raise ValueError(f"Table '{schema}.{table_name}' not found in the database")
        
        # Get column information
        cursor.execute("""
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key,
                col_description((quote_ident(%s) || '.' || quote_ident(%s))::regclass, 
                               c.ordinal_position) as column_description
            FROM
                information_schema.columns c
            LEFT JOIN (
                SELECT
                    kcu.column_name
                FROM
                    information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_schema = kcu.constraint_schema
                        AND tc.constraint_name = kcu.constraint_name
                WHERE
                    tc.constraint_type = 'PRIMARY KEY' AND
                    tc.table_schema = %s AND
                    tc.table_name = %s
            ) pk ON c.column_name = pk.column_name
            WHERE
                c.table_schema = %s AND
                c.table_name = %s
            ORDER BY
                c.ordinal_position
        """, (schema, table_name, schema, table_name, schema, table_name))
        
        columns = []
        for row in cursor.fetchall():
            column_name, data_type, char_max_len, num_precision, num_scale, is_nullable, is_pk, description = row
            
            # Format the complete data type with precision/scale if applicable
            if char_max_len is not None:
                full_data_type = f"{data_type}({char_max_len})"
            elif num_precision is not None and num_scale is not None:
                full_data_type = f"{data_type}({num_precision},{num_scale})"
            elif num_precision is not None:
                full_data_type = f"{data_type}({num_precision})"
            else:
                full_data_type = data_type
                
            columns.append({
                "name": column_name,
                "data_type": full_data_type,
                "base_type": data_type,
                "max_length": char_max_len,
                "precision": num_precision,
                "scale": num_scale,
                "is_required": is_nullable == 'NO',
                "is_primary_key": is_pk,
                "description": description
            })
        
        # Get table description
        cursor.execute("""
            SELECT 
                obj_description((quote_ident(%s) || '.' || quote_ident(%s))::regclass, 'pg_class')
        """, (schema, table_name))
        
        table_description = cursor.fetchone()[0]
        
        # Close connection
        conn.close()
        
        # Return metadata dictionary
        return {
            "table_name": table_name,
            "schema": schema,
            "server_type": "postgres",
            "description": table_description,
            "columns": columns
        }
            
    except Exception as e:
        raise RuntimeError(f"Error retrieving PostgreSQL table metadata: {str(e)}")
