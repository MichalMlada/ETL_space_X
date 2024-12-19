import os
from extraction.fetch_data import fetch_data
from extraction.save_data import save_data_to_file
from extraction.schema import fetch_schema, save_schema_to_file

SPACEX_API_URL = "https://api.spacexdata.com/v4/"

def fetch_and_process_data(source_url, table_name):
    save_data = "data"  # Directory to save the data
    save_schema = "schema"  # Directory to save the schema

    try:
        # Fetch and save the data
        full_url = SPACEX_API_URL + source_url
        data = fetch_data(full_url)
        save_data_to_file(data, save_data, table_name)

        # Raw GitHub URL for schema (change this to the raw URL)
        full_url_schema = f"https://raw.githubusercontent.com/r-spacex/SpaceX-API/master/docs/{table_name}/v4/schema.md"
        schema = fetch_schema(full_url_schema)  # Fetch raw markdown schema
        schema_name = table_name + "_schema"  # Add '_schema' suffix to the table name
        save_schema_to_file(schema, save_schema, schema_name)  # Save the raw markdown schema

    except Exception as e:
        print(f"Error during data processing for {table_name}: {e}")

def main():
    # Define datasets with table names and their corresponding API source URLs
    datasets = [
        {'table_name': 'launches', 'source_url': 'launches'},
        {'table_name': 'payloads', 'source_url': 'payloads'},
        {'table_name': 'capsules', 'source_url': 'capsules'},  # Example for capsules
        # Add other tables here as needed
    ]

    for dataset in datasets:
        table_name = dataset['table_name']
        source_url = dataset['source_url']

        # Fetch and process the data for each table
        fetch_and_process_data(source_url, table_name)

if __name__ == '__main__':
    main()
