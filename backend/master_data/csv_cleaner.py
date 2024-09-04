import os
import pandas as pd
import logging
from csv_standardizer import CSVstandarizer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CSVCleaner:

    @staticmethod
    def process_all_csv_files_in_folder(folder_path):
        """
        Applies conversion to lowercase, cleans column names, and validates/cleans data
        to all CSV files in the specified folder.

        Parameters:
        folder_path (str): The path to the folder containing CSV files.
        """
        if not os.path.isdir(folder_path):
            logging.error(f"Folder path does not exist: {folder_path}")
            return

        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                logging.info(f"Processing file: {file_path}")

                try:
                    # Convert all string entries to lowercase
                    CSVstandarizer.convert_csv_to_lowercase(file_path)

                    # Clean column names
                    CSVstandarizer.clean_column_names(file_path)

                    # Standarize phone numbers
                    CSVstandarizer.convert_phone_numbers(file_path, "phone")

                    # Validate and clean CSV data
                    CSVstandarizer.validate_and_clean_csv(file_path)



                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")

    @staticmethod
    def combine_csv_files(folder_path, output_file_path):
        """
        Combines all CSV files in a specified folder into a single CSV file.

        Parameters:
        folder_path (str): The path to the folder containing the CSV files.
        output_file_path (str): The path where the combined CSV file will be saved.
        """
        expected_columns = ['#', 'agent_id', 'first_name', 'last_name', 'office_id', 'office_name',
                            'office_address', 'office_city', 'office_zip', 'office_county',
                            'phone_1', 'phone_1_type', 'phone_2', 'phone_2_type', 'phone_3',
                            'phone_3_type', 'email', "alt_address","alt_city","alt_zip"]

        if not os.path.isdir(folder_path):
            logging.error(f"Folder path does not exist: {folder_path}")
            return

        all_dfs = []

        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                logging.info(f"Processing file: {file_path}")

                try:
                    df = pd.read_csv(file_path)

                    # Verify that the DataFrame has the expected columns
                    if all(col in df.columns for col in expected_columns):
                        df = df[expected_columns]
                        all_dfs.append(df)
                    else:
                        logging.warning(f"File {file_path} does not have the expected columns.")

                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            combined_df.to_csv(output_file_path, index=False)
            logging.info(f"Combined file saved to: {output_file_path}")
        else:
            logging.warning("No CSV files to combine.")

CSVCleaner.process_all_csv_files_in_folder(r'backend\agentdata\new_data')
CSVCleaner.combine_csv_files(r'backend\agentdata\new_data', r'backend\agentdata\new_data\new_added_data.csv')
