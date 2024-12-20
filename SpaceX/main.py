import os
from extraction.fetch_data import fetch_data
from extraction.save_data import save_data_to_file
from extraction.schema import fetch_schema, save_schema_to_file
from loading.database_operations import DatabaseOperations
from dotenv import load_dotenv

SPACEX_API_URL = "https://api.spacexdata.com/v4/"

load_dotenv(dotenv_path='.env')
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}


def fetch_and_process_data(source_url, table_name, db_ops):
    save_data = "data"
    save_schema = "schema"

    try:
        # Fetch and save the data
        full_url = SPACEX_API_URL + source_url
        data = fetch_data(full_url)
        save_data_to_file(data, save_data, table_name)

        # Fetch and save schema
        full_url_schema = f"https://raw.githubusercontent.com/r-spacex/SpaceX-API/master/docs/{table_name}/v4/schema.md"
        schema = fetch_schema(full_url_schema)
        schema_name = table_name + "_schema"
        save_schema_to_file(schema, save_schema, schema_name)

        # Create table and insert data
        schema_file = os.path.join(save_schema, f"{schema_name}.json")
        data_file = os.path.join(save_data, f"{table_name}.json")
        
        db_ops.create_table_if_not_exists(table_name, schema_file)
        db_ops.insert_or_update(table_name, data_file)

    except Exception as e:
        print(f"Error during data processing for {table_name}: {e}")

def main():
    # Initialize database operations
    db_ops = DatabaseOperations(DB_PARAMS)

    # Define datasets
    datasets = [
        {'table_name': 'launches', 'source_url': 'launches'},
        {'table_name': 'payloads', 'source_url': 'payloads'},
        {'table_name': 'capsules', 'source_url': 'capsules'},
    ]

    for dataset in datasets:
        table_name = dataset['table_name']
        source_url = dataset['source_url']
        fetch_and_process_data(source_url, table_name, db_ops)

if __name__ == '__main__':
    main()