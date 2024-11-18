import pandas as pd
import os
import re

# Function to format phone numbers
def format_phone_number(phone):
    """
    Cleans and validates a phone number by removing non-numeric characters.
    Returns the phone number only if it is 10 digits long, otherwise None.
    """
    phone = re.sub(r'\D', '', str(phone))  # Remove non-numeric characters
    return phone if len(phone) == 10 else None

# Combine CSV files from a folder into a master file
def combine_csv_files(folder_path, master_csv_path, expected_columns):
    """
    Combines all CSV files in a folder into a master CSV file.

    Parameters:
    - folder_path: Path to the folder containing CSV files.
    - master_csv_path: Path to save the combined master CSV file.
    - expected_columns: List of columns to ensure consistency in the combined data.
    """
    dataframes = []

    # Iterate through each CSV file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {filename}")

            # Read each CSV file
            try:
                df = pd.read_csv(file_path)
                # Ensure it has the expected columns in the correct order
                df = df[expected_columns]
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                continue

            # Format phone numbers for specified columns
            phone_columns = ["Phone 1", "Phone 2", "Phone 3"]
            for phone_col in phone_columns:
                if phone_col in df.columns:
                    df[phone_col] = df[phone_col].apply(format_phone_number)

            # Append the cleaned DataFrame to the list
            dataframes.append(df)

    # Combine all DataFrames into a single DataFrame
    if dataframes:
        master_data = pd.concat(dataframes, ignore_index=True)
        # Save the combined data to the master CSV file
        try:
            master_data.to_csv(master_csv_path, index=False)
            print(f"Master CSV file created at: {master_csv_path}")
        except Exception as e:
            print(f"Error saving the master CSV file: {e}")
    else:
        print("No valid CSV files found to combine.")

# Main script execution
if __name__ == "__main__":
    # Define folder and file paths
    FOLDER_PATH = r"backend\data\brokermetrics_data\Newly_added"
    MASTER_CSV_PATH = r"backend\data\brokermetrics_data\Master\new.csv"

    # Define expected columns
    EXPECTED_COLUMNS = [
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

    # Call the combine_csv_files function
    combine_csv_files(FOLDER_PATH, MASTER_CSV_PATH, EXPECTED_COLUMNS)
