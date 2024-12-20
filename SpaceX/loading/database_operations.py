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
            'Number': 'NUMERIC',
            'Boolean': 'BOOLEAN',
            'UUID': 'UUID',
            'Date': 'TIMESTAMP',
            'Object': 'JSONB',
            'Array': 'JSONB'
        }
        
        sql_columns = []
        
        for field_name, field_info in schema_data.items():
            # Handle reserved words by quoting them
            quoted_field_name = f'"{field_name}"'
            
            # Check if field is an array
            if isinstance(field_info, list):
                sql_type = 'JSONB'
            else:
                # Get the field type
                field_type = field_info.get('type', 'String')
                
                # Handle UUID fields
                if field_type == 'UUID':
                    sql_type = 'UUID'
                # Handle nested objects and arrays
                elif isinstance(field_type, (dict, list)) or field_type == 'Object':
                    sql_type = 'JSONB'
                else:
                    sql_type = type_mapping.get(field_type, 'TEXT')
                
                # Add constraints
                constraints = []
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
            
        return sql_columns

    def create_table_if_not_exists(self, table_name, schema_file):
        """Create table if it doesn't exist based on the schema"""
        try:
            # Load schema from file
            with open(schema_file, 'r') as f:
                schema_data = json.load(f)
            
            # Convert schema to SQL column definitions
            sql_columns = self._convert_schema_to_sql_types(schema_data)
            
            # Create table SQL
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(sql_columns)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            print(f"Creating table {table_name}...")
            print(create_table_sql)
            
            # Execute creation
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Create UUID extension if not exists
                    cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
                    # Create the table
                    cur.execute(create_table_sql)
                    conn.commit()
                    
            print(f"Table {table_name} created or already exists")
            
        except Exception as e:
            print(f"Error creating table {table_name}: {str(e)}")
            raise

    def insert_or_update(self, table_name, data_file):
        """Insert or update data from JSON file into the specified table"""
        try:
            # Load data from file
            with open(data_file, 'r') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                data = [data]
            
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    # Get column names
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                        AND column_name != 'id'
                        AND column_name != 'created_at';
                    """)
                    columns = [row[0] for row in cur.fetchall()]
                    
                    for record in data:
                        # Prepare data for insertion
                        values = []
                        for col in columns:
                            # Handle nested data by converting to JSONB
                            value = record.get(col)
                            if isinstance(value, (dict, list)):
                                value = Json(value)
                            values.append(value)
                        
                        # Create placeholders for the SQL query
                        placeholders = ','.join(['%s'] * len(columns))
                        columns_str = ','.join([f'"{col}"' for col in columns])
                        
                        # Create UPSERT query using serial as the unique identifier
                        insert_sql = f"""
                            INSERT INTO {table_name} ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT ("serial") DO UPDATE
                            SET ({columns_str}) = ({placeholders})
                        """
                        
                        cur.execute(insert_sql, values * 2)  # * 2 because we need values twice for ON CONFLICT
                    
                    conn.commit()
                    
            print(f"Data successfully inserted/updated in {table_name}")
            
        except Exception as e:
            print(f"Error inserting/updating data in {table_name}: {str(e)}")
            raise