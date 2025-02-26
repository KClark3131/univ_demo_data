import pandas as pd

# Read the CSV file using a raw string and specify data types
df = pd.read_csv(r'YOUR FILE HERE', dtype={'column1': str, 'column7': str, 'column9': str}, low_memory=False)

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

# LETS KEEP CLEANING!!
# Read the target CSV file
target_df = pd.read_csv(r'YOUR FILE HERE"', dtype={'institution': str}, low_memory=False)

# Read the source Excel file
source_df = pd.read_excel(r'YOUR FILE HERE', dtype={'institution': str})

# Ensure the column names are consistent
target_df.columns = [col.lower() for col in target_df.columns]
source_df.columns = [col.lower() for col in source_df.columns]

# Merge the DataFrames on the 'institution' column
merged_df = target_df.merge(source_df[['institution', 'label']], on='institution', how='left')

# Print the columns of the merged DataFrame for debugging
print("Columns in merged DataFrame:", merged_df.columns)

# Update the target DataFrame with the values from the source DataFrame
target_df['label'] = merged_df['label_y'].combine_first(merged_df['label_x'])

# Save the updated DataFrame to a new CSV file
target_df.to_csv('updated_clean_pseo_flows_data.csv', index=False)
print("Data has been updated and saved to updated_clean_pseo_flows_data.csv")

# LET'S MERGE SOME STUFF!!! 
# Read the target CSV file
df1 = pd.read_csv(r"C:\Users\kclar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.13\pseo_flows_data.csv", dtype={'institution_code': str}, low_memory=False)

# Read the key file that maps institution_code to institution_name
df_key = pd.read_excel(r"C:\Users\kclar\OneDrive\Desktop\Univdata\Data_Univ_by_State\key_dictionaries\institution_key_dictionary.xlsx", dtype={'institution_code': str})

# Ensure the column names are consistent
df1.columns = [col.lower() for col in df1.columns]
df_key.columns = [col.lower() for col in df_key.columns]

# Print the columns of each DataFrame for debugging
print("Columns in df1:", df1.columns)
print("Columns in df_key:", df_key.columns)

# Merge df1 with df_key based on the 'institution_code' column
merged_df = pd.merge(df1, df_key[['institution_code', 'institution_name']], left_on="institution_code", right_on="institution_code", how="left")

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after merge:", merged_df.columns)

# Reorder columns to place 'institution_name' next to 'institution_code'
cols = list(merged_df.columns)
institution_code_index = cols.index('institution_code')
cols.insert(institution_code_index + 1, cols.pop(cols.index('institution_name')))
merged_df = merged_df[cols]

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after reordering:", merged_df.columns)

# Drop the 'institution_code' column as it's no longer needed
merged_df.drop(columns=['institution_code'], inplace=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after dropping 'institution_code':", merged_df.columns)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('updated_pseo_flows_data.csv', index=False)

# Print a message indicating the merge is complete
print("The 'institution_name' column has been added next to 'institution_code'.")

# Read the target CSV file
df1 = pd.read_csv(r"C:\Users\kclar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.13\updated_pseo_flows_data_with_degree_type.csv", dtype={'cipcode': str}, low_memory=False)

# Read the key file that maps cipcode to cip_label
df_key = pd.read_excel(r"C:\Users\kclar\OneDrive\Desktop\Univdata\Data_Univ_by_State\key_dictionaries\cip_code_key_dictionary.xlsx", dtype={'cipcode': str})

# Ensure the column names are consistent
df1.columns = [col.lower() for col in df1.columns]
df_key.columns = [col.lower() for col in df_key.columns]

# Print the columns of each DataFrame for debugging
print("Columns in df1:", df1.columns)
print("Columns in df_key:", df_key.columns)

# Check for duplicates in the key columns
print("Duplicates in df1 cipcode:", df1['cipcode'].duplicated().sum())
print("Duplicates in df_key cipcode:", df_key['cipcode'].duplicated().sum())

# Ensure there are no duplicate keys in df_key
df_key = df_key.drop_duplicates(subset=['cipcode'])

chunk_size = 100000  # Adjust based on memory limits
merged_chunks = []

# Merge in chunks to handle large DataFrames
for start in range(0, len(df1), chunk_size):
    chunk = df1.iloc[start:start + chunk_size]
    merged_chunk = pd.merge(
        chunk,
        df_key[['cipcode', 'degree_label']],
        left_on="cipcode",
        right_on="cipcode",
        how="left"
    )
    merged_chunks.append(merged_chunk)

# Concatenate all merged chunks
merged_df = pd.concat(merged_chunks, ignore_index=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after merging cipcode:", merged_df.columns)

# Rename the 'cipcode' column to 'cip_label'
merged_df.rename(columns={'cipcode': 'degree_label'}, inplace=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after renaming cipcode to degree_label:", merged_df.columns)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('updated_pseo_flows_data_with_cip_label.csv', index=False)

# Print a message indicating the merge is complete
print("The 'cip_label' column has been added and the 'cipcode' column has been relabeled to 'degree_label'.")

# Read the target CSV file
df1 = pd.read_csv(r"C:\Users\kclar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.13\updated_pseo_flows_data_with_naics.csv", dtype={'degree_code': str}, low_memory=False)

# Read the key file that maps degree_code to degree_type
df_key = pd.read_excel(r"C:\Users\kclar\OneDrive\Desktop\Univdata\Data_Univ_by_State\key_dictionaries\degree_key_dictionary.xlsx", dtype={'degree_code': str})

# Ensure the column names are consistent
df1.columns = [col.lower() for col in df1.columns]
df_key.columns = [col.lower() for col in df_key.columns]

# Print the columns of each DataFrame for debugging
print("Columns in df1:", df1.columns)
print("Columns in df_key:", df_key.columns)

# Check for duplicates in the key columns
print("Duplicates in df1 degree_code:", df1['degree_code'].duplicated().sum())
print("Duplicates in df_key degree_code:", df_key['degree_code'].duplicated().sum())

# Ensure there are no duplicate keys in df_key
df_key = df_key.drop_duplicates(subset=['degree_code'])

chunk_size = 100000  # Adjust based on memory limits
merged_chunks = []

# Merge in chunks to handle large DataFrames
for start in range(0, len(df1), chunk_size):
    chunk = df1.iloc[start:start + chunk_size]
    merged_chunk = pd.merge(
        chunk,
        df_key[['degree_code', 'degree_type']],
        left_on="degree_code",
        right_on="degree_code",
        how="left"
    )
    merged_chunks.append(merged_chunk)

# Concatenate all merged chunks
merged_df = pd.concat(merged_chunks, ignore_index=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after merging degree_code:", merged_df.columns)

# Rename the 'degree_code' column to 'degree_type'
merged_df.rename(columns={'degree_code': 'degree_type'}, inplace=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after renaming degree_code to degree_type:", merged_df.columns)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('updated_pseo_flows_data_with_degree_type.csv', index=False)

# Print a message indicating the merge is complete
print("The 'degree_type' column has been added and the 'degree_code' column has been relabeled to 'degree_type'.")

# Read the target CSV file
df1 = pd.read_csv(r"C:\Users\kclar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.13\updated_pseo_flows_data.csv", dtype={'institution_state_code': str}, low_memory=False)

# Read the key file that maps institution_state_code to institution_state
df_key = pd.read_excel(r"C:\Users\kclar\OneDrive\Desktop\Univdata\Data_Univ_by_State\key_dictionaries\institution_key_dictionary.xlsx", dtype={'institution_state_code': str})

# Ensure the column names are consistent
df1.columns = [col.lower() for col in df1.columns]
df_key.columns = [col.lower() for col in df_key.columns]

# Print the columns of each DataFrame for debugging
print("Columns in df1:", df1.columns)
print("Columns in df_key:", df_key.columns)

# Check for duplicates in the key columns
print("Duplicates in df1 institution_state_code:", df1['institution_state_code'].duplicated().sum())
print("Duplicates in df_key institution_state_code:", df_key['institution_state_code'].duplicated().sum())

# Ensure there are no duplicate keys in df_key
df_key = df_key.drop_duplicates(subset=['institution_state_code'])

chunk_size = 100000  # Adjust based on memory limits
merged_chunks = []

# Merge in chunks to handle large DataFrames
for start in range(0, len(df1), chunk_size):
    chunk = df1.iloc[start:start + chunk_size]
    merged_chunk = pd.merge(
        chunk,
        df_key[['institution_state_code', 'institution_state']],
        left_on="institution_state_code",
        right_on="institution_state_code",
        how="left"
    )
    merged_chunks.append(merged_chunk)

# Concatenate all merged chunks
merged_df = pd.concat(merged_chunks, ignore_index=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after merging institution_state_code:", merged_df.columns)

# Reorder columns to place 'institution_state' next to 'institution_state_code'
cols = list(merged_df.columns)
institution_state_code_index = cols.index('institution_state_code')
cols.insert(institution_state_code_index + 1, cols.pop(cols.index('institution_state')))
merged_df = merged_df[cols]

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after reordering institution_state:", merged_df.columns)

# Drop the 'institution_state_code' column if it's no longer needed
merged_df.drop(columns=['institution_state_code'], inplace=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after dropping 'institution_state_code':", merged_df.columns)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('updated_pseo_flows_data_with_state.csv', index=False)

# Print a message indicating the merge is complete
print("The 'institution_state' column has been added next to 'institution_state_code'.")
# Read the target CSV file
df1 = pd.read_csv(r"C:\Users\kclar\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python 3.13\updated_pseo_flows_data_with_state.csv", dtype={'naics_industry_code': str}, low_memory=False)

# Read the key file that maps naics_industry_code to naics_industry
df_key = pd.read_excel(r"C:\Users\kclar\OneDrive\Desktop\Univdata\Data_Univ_by_State\key_dictionaries\naics_codes_key_dictionary.xlsx", dtype={'naics_industry_code': str})

# Ensure the column names are consistent
df1.columns = [col.lower() for col in df1.columns]
df_key.columns = [col.lower() for col in df_key.columns]

# Print the columns of each DataFrame for debugging
print("Columns in df1:", df1.columns)
print("Columns in df_key:", df_key.columns)

# Check for duplicates in the key columns
print("Duplicates in df1 naics_industry_code:", df1['naics_industry_code'].duplicated().sum())
print("Duplicates in df_key naics_industry_code:", df_key['naics_industry_code'].duplicated().sum())

# Ensure there are no duplicate keys in df_key
df_key = df_key.drop_duplicates(subset=['naics_industry_code'])

chunk_size = 100000  # Adjust based on memory limits
merged_chunks = []

# Merge in chunks to handle large DataFrames
for start in range(0, len(df1), chunk_size):
    chunk = df1.iloc[start:start + chunk_size]
    merged_chunk = pd.merge(
        chunk,
        df_key[['naics_industry_code', 'naics_industry_title']],
        left_on="naics_industry_code",
        right_on="naics_industry_code",
        how="left"
    )
    merged_chunks.append(merged_chunk)

# Concatenate all merged chunks
merged_df = pd.concat(merged_chunks, ignore_index=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after merging naics_industry_code:", merged_df.columns)

# Reorder columns to place 'naics_industry' next to 'naics_industry_code'
cols = list(merged_df.columns)
naics_industry_code_index = cols.index('naics_industry_code')
cols.insert(naics_industry_code_index + 1, cols.pop(cols.index('naics_industry_title')))
merged_df = merged_df[cols]

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after reordering naics_industry:", merged_df.columns)

# Drop the 'naics_industry_code' column if it's no longer needed
merged_df.drop(columns=['naics_industry_code'], inplace=True)

# Print the columns of the merged DataFrame for debugging
print("Columns in merged_df after dropping 'naics_industry_code':", merged_df.columns)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('updated_pseo_flows_data_with_naics.csv', index=False)

# Print a message indicating the merge is complete
print("The 'naics_industry' column has been added next to 'naics_industry_code'.")
