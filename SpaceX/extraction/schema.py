# extraction/schema.py
import requests
import os
import json
import re

def fetch_schema(github_url):
    """Fetch schema from a raw GitHub URL."""
    try:
        print(f"Fetching schema from {github_url}...")
        response = requests.get(github_url)
        response.raise_for_status()  # Raise error for invalid responses
        print("Schema fetched successfully.")
        return response.text  # Return the schema as raw markdown text
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise

def extract_json_from_markdown(markdown_text):
    """Extract and clean up JSON from markdown formatted text."""
    # Use regular expression to find the JSON block within the markdown
    json_match = re.search(r"```json\n(.*?)\n```", markdown_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)  # Extract JSON string
        
        # Replace any single quotes with double quotes
        json_str = json_str.replace("'", '"')
        
        # Remove trailing commas (if any)
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        
        try:
            # Try parsing the cleaned JSON string
            json_data = json.loads(json_str)
            return json_data
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            raise
    else:
        print("No JSON block found in the markdown.")
        raise ValueError("No JSON block found in the markdown.")

def save_schema_to_file(data, save_path, name):

    data = extract_json_from_markdown(data)
    """Save fetched JSON data to a file in the specified path."""
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    file_path = os.path.join(save_path, f"{name}.json")
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {file_path}")
