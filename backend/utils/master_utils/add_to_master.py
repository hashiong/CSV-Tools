import pandas as pd



# Define file paths
new_csv_path = r"backend\data\brokermetrics_data\Master\new.csv"
master_csv_path = r"backend\data\brokermetrics_data\Master\10232024.csv" 
new_master_csv_path = r"backend\data\brokermetrics_data\Master\11132024.csv"

# Load the new data and the master data
new_data = pd.read_csv(new_csv_path)
master_data = pd.read_csv(master_csv_path)

# Ensure both have the same columns and order
columns = [
    "Agent ID",
    "First Name",
    "Last Name",
    "Office ID",
    "Office Name",
    "Phone 1",
    "Phone 1 Type",
    "Phone 2",
    "Phone 2 Type",
    "Phone 3",
    "Phone 3 Type",
    "EMail",
    "Alt. Address",
    "Alt. City",
    "Alt. Zip"
]

new_data = new_data[columns]
master_data = master_data[columns]

# Append new data to master data
updated_master_data = pd.concat([master_data, new_data], ignore_index=True)

# Remove rows where Phone 1, Phone 2, and Phone 3 are all empty (NaN)
updated_master_data = updated_master_data.dropna(subset=["Phone 1", "Phone 2", "Phone 3"], how="all")

# Save the updated master file
updated_master_data.to_csv(new_master_csv_path, index=False)

print("New data has been added to the master CSV.")
