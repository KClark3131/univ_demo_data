import requests
import csv

print("Script started")

# Insert API Key
api_key = "insert API key here"

# URL to get the list of variables
metadata_url = "https://api.census.gov/data/timeseries/pseo/flows/variables.json"

print("Fetching metadata...")

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

# Use valid variables from the list
valid_variables = ["CIP_LEVEL","CIPCODE", "DEGREE_LEVEL", "DIVISION", "GEOCOMP", "GEO_ID", "GRAD_COHORT", "GRAD_COHORT_YEARS", "INST_LEVEL", "INSTITUTION", "INST_STATE", "NAICS", "NATION", "SUMLEVEL", "Y1_GRADS_EMP", "Y1_GRADS_EMP_INSTATE", "Y1_GRADS_NME", "Y10_GRADS_EMP", "Y10_GRADS_EMP_INSTATE", "Y10_GRADS_NME", "Y5_GRADS_EMP", "Y5_GRADS_EMP_INSTATE", "Y5_GRADS_NME"]

# URL to get the data for the entire United States
url = f"https://api.census.gov/data/timeseries/pseo/flows?get=NAME,{','.join(valid_variables)}&for=us:*&key={api_key}"

# Print the URL to debug
print("Request URL:", url)

print("Fetching data...")

# Get the data
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data)
    
    # Convert the data to CSV
    with open('pseo_flows_data.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header
        writer.writerow(data[0])
        # Write the data rows
        writer.writerows(data[1:])
    print("Data has been written to pseo_flows_data.csv")
else:
    print("Error fetching data:", response.status_code)
    print("Response content:", response.content)

print("Script finished")
