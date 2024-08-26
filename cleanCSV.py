import os
from CSVstandarizer import CSVstandarizer
import pandas as pd


# columns: #,agent_id,first_name,last_name,office_id,office_name,office_address,office_city,office_zip,office_county,phone_1,phone_1_type,phone_2,phone_2_type,phone_3,phone_3_type,email,country
# reverse list columns: First Name,Last Name,Email,Office Name,Phone

def process_all_csv_files_in_folder(folder_path):
    """
    Applies conversion to lowercase, cleans column names, and validates/cleans data
    to all CSV files in the specified folder.

    Parameters:
    folder_path (str): The path to the folder containing CSV files.
    """
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)

            print(f"Processing file: {file_path}")

            # Convert all string entries to lowercase
            CSVstandarizer.convert_csv_to_lowercase(file_path)

            # Clean column names
            CSVstandarizer.clean_column_names(file_path)

            # Validate and clean CSV data
            CSVstandarizer.validate_and_clean_csv(file_path)

def combine_csv_files(folder_path, output_file_path):
    """
    Combines all CSV files in a specified folder into a single CSV file.

    Parameters:
    folder_path (str): The path to the folder containing the CSV files.
    output_file_path (str): The path where the combined CSV file will be saved.
    """
    # Define the expected columns
    expected_columns = ['#', 'agent_id', 'first_name', 'last_name', 'office_id', 'office_name',
                        'office_address', 'office_city', 'office_zip', 'office_county',
                        'phone_1', 'phone_1_type', 'phone_2', 'phone_2_type', 'phone_3',
                        'phone_3_type', 'email']

    # Initialize a list to hold DataFrames
    all_dfs = []

    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            print(f"Processing file: {file_path}")

            # Load the CSV file
            df = pd.read_csv(file_path)

            # Ensure the columns match the expected columns
            df = df[expected_columns]
            
            # Append to the list of DataFrames
            all_dfs.append(df)

    # Concatenate all DataFrames
    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file_path, index=False)
    print(f"Combined file saved to: {output_file_path}")

if __name__ == "__main__":
    folder = "data"  # Replace with your folder path
    CSVstandarizer.add_col_to_csv(file_path="Combined_CRMLS.csv", new_col_name="country", default_value="us")
