import pandas as pd
import os
import re

# Function to format phone number
def format_phone_number(phone):
    # Remove all non-numeric characters using regex
    phone = re.sub(r'\D', '', str(phone))
    return phone if len(phone) == 10 else None  # Return only if it's a valid 10-digit phone number

# Define the folder path and master CSV file path
folder_path = r"backend\data\brokermetrics_data\Newly_added"  # Replace with the path to your folder containing CSV files
master_csv_path = r"backend\data\brokermetrics_data\Master\new.csv"  # Replace with the path where you want to save the master CSV

# List to store individual DataFrames
dataframes = []

# Define expected columns
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

# Iterate through each CSV file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        print(filename)
        file_path = os.path.join(folder_path, filename)
        # Read each CSV file and ensure it has the correct columns
        df = pd.read_csv(file_path)
        df = df[columns]  # Ensure the DataFrame has the expected columns in the correct order
        
        # Format phone numbers for specified columns
        phone_columns = ["Phone 1", "Phone 2", "Phone 3"]
        for phone_col in phone_columns:
            df[phone_col] = df[phone_col].apply(format_phone_number)

        # Append to the list of dataframes
        dataframes.append(df)

# Concatenate all DataFrames into a single DataFrame
master_data = pd.concat(dataframes, ignore_index=True)

# Save the combined data to the master CSV file
master_data.to_csv(master_csv_path, index=False)

print("All CSV files in the folder have been combined into the master CSV.")
