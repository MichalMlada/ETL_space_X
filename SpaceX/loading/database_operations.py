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
            type_mapping = {
                'String': 'TEXT',
                'string': 'TEXT',
                'Number': 'NUMERIC',
                'number': 'NUMERIC',
                'Boolean': 'BOOLEAN',
                'boolean': 'BOOLEAN',
                'UUID': 'TEXT',
                'uuid': 'TEXT',
                'Date': 'TIMESTAMP',
                'date': 'TIMESTAMP',
                'Object': 'JSONB',
                'object': 'JSONB',
                'Array': 'JSONB',
                'array': 'JSONB'
            }

            sql_columns = []
            properties = schema_data.get('properties', schema_data.get('fields', schema_data))

            if not properties:
                raise ValueError(f"Could not find properties in schema. Schema keys: {list(schema_data.keys())}")

            for field_name, field_info in properties.items():
                quoted_field_name = f'"{field_name}"'

                field_type = field_info.get('type', 'TEXT').lower()
                sql_type = type_mapping.get(field_type, 'TEXT')

                constraints = []
                if field_info.get('required', False):
                    constraints.append('NOT NULL')
                if field_info.get('unique', False):
                    constraints.append('UNIQUE')

                default_value = field_info.get('default')
                if default_value is not None:
                    if isinstance(default_value, str):
                        constraints.append(f"DEFAULT '{default_value}'")
                    elif default_value is None:
                        constraints.append("DEFAULT NULL")
                    else:
                        constraints.append(f"DEFAULT {default_value}")

                sql_columns.append(f"{quoted_field_name} {sql_type} {' '.join(constraints)}")

            return sql_columns


    def create_table_if_not_exists(self, table_name, schema_file):
        try:
            with open(schema_file, 'r') as f:
                schema_data = json.load(f)

            sql_columns = self._convert_schema_to_sql_types(schema_data)
            if not sql_columns:
                raise ValueError("No columns generated from schema.")

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

            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {e}")
            raise

    def insert_or_update(self, table_name, data_file):
        try:
            with open(data_file, 'r') as f:
                data = json.load(f)

            if not isinstance(data, list):
                data = [data]

            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = '{table_name}'
                        AND column_name NOT IN ('id', 'created_at');
                    """)
                    columns = [row[0] for row in cur.fetchall()]
                    if not columns:
                        raise ValueError(f"No columns found for table {table_name}")

                    columns_str = ', '.join([f'"{col}"' for col in columns])
                    placeholders = ', '.join(['%s'] * len(columns))

                    for record in data:
                        values = [record.get(col) for col in columns]
                        insert_sql = f"""
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING;
                        """
                        cur.execute(insert_sql, values)
                    conn.commit()

            print(f"Data successfully inserted/updated in {table_name}.")
        except Exception as e:
            print(f"Error inserting/updating data in {table_name}: {e}")
            raise