import json
import psycopg2
from psycopg2.extras import Json
import os

class DatabaseOperations:
    def __init__(self, db_params):
        """
        Initialize database connection parameters
        db_params should be a dict with: dbname, user, password, host, port
        """
        self.db_params = db_params
        
    def _get_connection(self):
        """Create and return a database connection"""
        return psycopg2.connect(**self.db_params)

    def _convert_schema_to_sql_types(self, schema_data):
        """Convert JSON schema types to PostgreSQL types"""

        type_mapping = {
            'String': 'TEXT',
            'string': 'TEXT',
            'Number': 'NUMERIC',
            'number': 'NUMERIC',
            'Boolean': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            'UUID': 'JSONB',  # Changed to TEXT
            'uuid': 'JSONB',  # Added lowercase variant
            'Date': 'TIMESTAMP',
            'date': 'TIMESTAMP',
            'Object': 'JSONB',
            'object': 'JSONB',
            'Array': 'JSONB',
            'array': 'JSONB'
        }
        
        sql_columns = []
        
        # Try to find properties in different possible schema formats
        properties = None
        if isinstance(schema_data, dict):
            if 'properties' in schema_data:
                properties = schema_data['properties']
            elif 'fields' in schema_data:
                properties = schema_data['fields']
            else:
                properties = schema_data  # Assume the schema itself is the properties object
                
        if not properties:
            raise ValueError(f"Could not find properties in schema. Schema keys: {list(schema_data.keys())}")
            
        print(f"Found {len(properties)} properties to process")  # Debug print
        
        for field_name, field_info in properties.items():
            print(f"Processing field: {field_name}, info: {field_info}")  # Debug print
            
            # Handle reserved words by quoting them
            quoted_field_name = f'"{field_name}"'
            
            # Handle different schema formats
            if isinstance(field_info, dict):
                if 'type' in field_info:
                    field_type = field_info['type']
                elif '$ref' in field_info:
                    field_type = 'TEXT'  # Handle references as TEXT
                else:
                    field_type = 'TEXT'  # Default to TEXT
            elif isinstance(field_info, list):
                field_type = 'JSONB'
            else:
                field_type = str(field_info)
            
            # Normalize field type
            field_type = field_type.lower() if isinstance(field_type, str) else 'text'
            
            # Get SQL type
            if field_type in ['object', 'array'] or isinstance(field_info, (dict, list)):
                sql_type = 'JSONB'
            else:
                sql_type = type_mapping.get(field_type, 'TEXT')
            
            # Add constraints
            constraints = []
            if isinstance(field_info, dict):
                if field_info.get('required', False):
                    constraints.append('NOT NULL')
                if field_info.get('unique', False):
                    constraints.append('UNIQUE')
                
                # Add default value if specified
                default_value = field_info.get('default')
                if default_value is not None:
                    if isinstance(default_value, str):
                        constraints.append(f"DEFAULT '{default_value}'")
                    elif default_value is None:
                        constraints.append("DEFAULT NULL")
                    else:
                        constraints.append(f"DEFAULT {default_value}")
            
            sql_columns.append(f"{quoted_field_name} {sql_type} {' '.join(constraints)}".strip())
        
        print(f"Generated {len(sql_columns)} SQL columns")  # Debug print
        return sql_columns

    def create_table_if_not_exists(self, table_name, schema_file):
        """Create table if it doesn't exist based on the schema"""
        try:
            print(f"Reading schema from: {schema_file}")  # Debug print
            with open(schema_file, 'r') as f:
                schema_data = json.load(f)
            
            sql_columns = self._convert_schema_to_sql_types(schema_data)
            
            if not sql_columns:
                raise ValueError(f"No columns were generated from schema. Schema data: {json.dumps(schema_data, indent=2)}")
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(sql_columns)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_sql)
                    conn.commit()
            
            print(f"Table {table_name} created successfully")
            
        except Exception as e:
            print(f"Error creating table {table_name}: {str(e)}")
            raise

    def insert_or_update(self, table_name, data_file):
        """Insert or update data from JSON file into the specified table"""
        try:
            with open(data_file, 'r') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                data = [data]
            
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Get column information
                    cur.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                        AND column_name NOT IN ('id', 'created_at');
                    """)
                    columns_info = {row[0]: row[1] for row in cur.fetchall()}
                    
                    if not columns_info:
                        raise ValueError(f"No columns found for table {table_name}")

                    
                    for record in data:
                        values = []
                        for col in columns_info.keys():
                            value = record.get(col)
                            
                            # Handle different data types
                            if isinstance(value, (dict, list)):
                                values.append(Json(value))
                            else:
                                values.append(value)
                        
                        columns_str = ','.join([f'"{col}"' for col in columns_info.keys()])
                        placeholders = ','.join(['%s'] * len(columns_info))
                        
                        # Determine unique field
                        unique_field = None
                        if 'serial' in columns_info:
                            unique_field = 'serial'
                        elif 'name' in columns_info:
                            unique_field = 'name'
                        
                        if unique_field:
                            insert_sql = f"""
                                INSERT INTO {table_name} ({columns_str})
                                VALUES ({placeholders})
                                ON CONFLICT ("{unique_field}") DO UPDATE
                                SET ({columns_str}) = ({placeholders});
                            """
                            cur.execute(insert_sql, values * 2)
                        else:
                            insert_sql = f"""
                                INSERT INTO {table_name} ({columns_str})
                                VALUES ({placeholders});
                            """
                            cur.execute(insert_sql, values)
                        
                    conn.commit()
            
            print(f"Data successfully inserted/updated in {table_name}")
            
        except Exception as e:
            print(f"Error inserting/updating data in {table_name}: {str(e)}")
            raise