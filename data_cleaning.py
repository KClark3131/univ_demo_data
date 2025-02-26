import pandas as pd

# Read the CSV file using a raw string and specify data types
df = pd.read_csv(r'C:\Users\kclar\AppData\Local\Programs\Python\clean_pseo_flows_data.csv', dtype={'column1': str, 'column7': str, 'column9': str}, low_memory=False)

# Convert column names to lowercase
df.columns = [col.lower() for col in df.columns]

# Strip whitespace from string columns
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Save the cleaned data
df.to_csv('cleaned_pseo_flows_data.csv', index=False)
print("Data has been cleaned and saved to cleaned_pseo_flows_data.csv")

# Read the cleaned CSV file with specified data types
data = pd.read_csv('cleaned_pseo_flows_data.csv', dtype={'column1': str, 'column7': str, 'column9': str}, low_memory=False)

# Print the data
print(data)
