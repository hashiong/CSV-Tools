import pandas as pd

# Load your CSV file into a pandas DataFrame
df = pd.read_csv('agentdata/reverse_prospect_data/reverse_prospect_agent_list.csv')
# Remove rows where the 'Name' column is empty or NaN
df = df.dropna(subset=['Name'])
# Split the "Member Email Office Name Member Direct Phone" column into separate columns
df[['email', 'office_name', 'member_direct_phone']] = df['MessData'].str.split('\n', expand=True)

def split_name(name):
    print(name)
    parts = name.split(' ', 1)
    
    return pd.Series(parts)

# Apply the function and create new columns
df[['first_name', 'last_name']] = df['Name'].apply(split_name)


# Save the modified DataFrame to a new CSV file
df.to_csv('modified_file.csv', index=False)
