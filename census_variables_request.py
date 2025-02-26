import requests

# Insert API Key
api_key = "YOUR KEY HERE"

# URL to get the list of variables
metadata_url = "https://api.census.gov/data/timeseries/pseo/flows/variables.json"

# Get the list of variables
metadata_response = requests.get(metadata_url)

if metadata_response.status_code == 200:
    metadata = metadata_response.json()
    variables = metadata['variables']
    print("List of variables:")
    for variable in variables:
        print(variable)
else:
    print("Error fetching metadata:", metadata_response.status_code)
