import pandas as pd

# Load the two CSV files
df1 = pd.read_csv(r'CSV-Tools\backend\agentdata\reverse_prospect_data\vicky_reverse_prospect.csv')  # The file containing first names and last names
df2 = pd.read_csv(r'CSV-Tools\backend\brokermetrics_data\Master\10212024.csv')  # The file to compare against

# Assuming the columns are named 'first_name' and 'last_name'
# Adjust the column names based on your CSV files

# Convert all cells in both DataFrames to lowercase
df1 = df1.applymap(lambda x: x.lower() if isinstance(x, str) else x)
df2 = df2.applymap(lambda x: x.lower() if isinstance(x, str) else x)

# Create a set of full names from the second CSV
full_names_set = set(df2['First Name'].astype(str) + ' ' + df2['Last Name'].astype(str))

# Create a column for full names in the first DataFrame
df1['full_name'] = df1['First Name'].astype(str) + ' ' + df1['Last Name'].astype(str)

# Find names in the first DataFrame that are not in the set from the second DataFrame
missing_names = df1[~df1['full_name'].isin(full_names_set)]

# Get the count of missing names
count_missing = missing_names.shape[0]

# Print the result
print(f"Number of first names and last names not found in the second file: {count_missing}")
