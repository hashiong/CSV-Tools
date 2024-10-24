import os
import pandas as pd
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class CSVProcessor:

    @staticmethod
    def load_csv(file_path):
        """Loads a CSV file into a DataFrame."""
        try:
            df = pd.read_csv(file_path)
            return df
        except FileNotFoundError:
            logging.error(f"Error: The file at {file_path} was not found.")
        except pd.errors.EmptyDataError:
            logging.error("Error: The file is empty.")
        except pd.errors.ParserError:
            logging.error(
                "Error: There was a parsing error. Check the file's format.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        return None

    @staticmethod
    def clean_column_names(df):
        """Cleans column names by removing leading/trailing spaces, converting to lowercase, and replacing spaces with underscores."""
        df.columns = df.columns.str.strip().str.lower(
        ).str.replace('.', '').str.replace(' ', '_')
        return df

    @staticmethod
    def convert_phone_numbers(df, phone_columns):
        """Converts phone numbers to pure digits in specified columns and ensures they are 10 digits long."""
        for column in phone_columns:
            if column in df.columns:
                # Convert to string and check for '.' at index -2
                df[column] = df[column].astype(str).apply(
                    lambda x: x[:-1] if len(x) == 12 and x[-2] == '.' else x
                )

                # Clean phone numbers: remove non-digit characters and validate length

                df[column] = df[column].str.replace(r'\D', '', regex=True)
  
                df[column] = df[column].apply(lambda x: x if len(x) == 10 else None)  # Set to None if not 10 digits
                
                # Optionally, convert the column back to string type
                df[column] = df[column].astype('string')  # Ensure the column is of string type for further operations

        return df


    @staticmethod
    def validate_and_clean_csv(df, validation_rules):
        """Validates the data in the DataFrame according to predefined rules and cleans invalid entries."""
        columns_to_keep = [col for col in df.columns if col in validation_rules]
        df = df[columns_to_keep]
        for col, pattern in validation_rules.items():
            if col in df.columns:
                invalid_mask = ~df[col].astype(str).str.match(pattern)
                df.loc[invalid_mask, col] = np.nan  # Set invalid cells to NaN
        return df

    @staticmethod
    def convert_csv_to_lowercase(df):
        """Converts all string entries in the DataFrame to lowercase."""
        return df.applymap(lambda s: s.strip().lower() if isinstance(s, str) else s)

    @staticmethod
    def save_csv(df, file_path, index=False):
        """Saves a pandas DataFrame to a CSV file."""
        try:
            df.to_csv(file_path, index=index)
            logging.info(f"File saved successfully at {file_path}")
        except Exception as e:
            logging.error(f"An error occurred while saving the file: {e}")

    @staticmethod
    def combine_csv_files(folder_path, output_file_path):
        """Combines all CSV files in a specified folder into a single CSV file."""
        expected_columns = ['Agent ID', 'First Name', 'Last Name', 'Office ID', 'Office Name',
       'Phone 1', 'Phone 1 Type', 'Phone 2', 'Phone 2 Type', 'Phone 3',
       'Phone 3 Type', 'EMail', 'Alt. Address', 'Alt. City', 'Alt. Zip']

        if not os.path.isdir(folder_path):
            logging.error(f"Folder path does not exist: {folder_path}")
            return

        all_dfs = []
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                logging.info(f"Processing file: {file_path}")

                try:
                    df = CSVProcessor.load_csv(file_path)
                    if df is not None and all(col in df.columns for col in expected_columns):
                        # df = CSVProcessor.clean_column_names(df)
                        df = CSVProcessor.convert_phone_numbers(df, ['Phone 1', 'Phone 2', 'Phone 3'])
                        all_dfs.append(df[expected_columns])
                    else:
                        logging.warning(f"File {file_path} does not have the expected columns or could not be loaded.")
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            CSVProcessor.save_csv(combined_df, output_file_path)
        else:
            logging.warning("No CSV files to combine.")

    @staticmethod
    def combine_two_csv(file_path1, file_path2, output_file_path):
        """Combines two CSV files into a single CSV file."""
        try:
            df1 = CSVProcessor.load_csv(file_path1)
            df2 = CSVProcessor.load_csv(file_path2)

            expected_columns = ['Agent ID', 'First Name', 'Last Name', 'Office ID', 'Office Name',
       'Phone 1', 'Phone 1 Type', 'Phone 2', 'Phone 2 Type', 'Phone 3',
       'Phone 3 Type', 'EMail', 'Alt. Address', 'Alt. City', 'Alt. Zip']

            if df2 is not None and all(col in df2.columns for col in expected_columns):
                # df = CSVProcessor.clean_column_names(df)
                df2 = CSVProcessor.convert_phone_numbers(df2, ['Phone 1', 'Phone 2', 'Phone 3'])

            if df1 is not None and df2 is not None:
                combined_df = pd.concat([df1, df2], ignore_index=True)
                CSVProcessor.save_csv(combined_df, output_file_path)
                logging.info(f"Successfully combined {file_path1} and {file_path2} into {output_file_path}")
            else:
                logging.warning("One or both CSV files could not be loaded.")

        except Exception as e:
            logging.error(f"Error combining CSV files: {e}")


    @staticmethod
    def clean_phone_numbers(file_path, phone_columns):
        """Cleans phone numbers in the specified columns of a CSV file."""
        df = CSVProcessor.load_csv(file_path)
        if df is not None:
            df = CSVProcessor.convert_phone_numbers(df, phone_columns)
            CSVProcessor.save_csv(df, file_path)

    @staticmethod
    def count_cells(file_path):
        """Counts the total number of non-empty phone cells in a CSV file."""
        df = CSVProcessor.load_csv(file_path)
        if df is not None:
            phone_columns = ["Phone 1", "Phone 2", "Phone 3"]
            non_empty_cells = df[phone_columns].notna().sum().sum()
            logging.info(f"Total number of phone cells in {phone_columns}: {non_empty_cells}")

    @staticmethod
    def drop_duplicates_in_csv(file_path):
        """Drops duplicate rows from a CSV file."""
        df = CSVProcessor.load_csv(file_path)
        if df is not None:
            row_count_before = len(df)
            logging.info(f'Row count before dropping duplicates: {row_count_before}')
            df_cleaned = df.drop_duplicates()
            row_count_after = len(df_cleaned)
            logging.info(f'Row count after dropping duplicates: {row_count_after}')
            CSVProcessor.save_csv(df_cleaned, file_path)



CSVProcessor.combine_csv_files(
    r'data\brokermetrics_data\LA',
    r'data\brokermetrics_data\Aggregated\LA.csv'
)

CSVProcessor.drop_duplicates_in_csv(
    r"data\brokermetrics_data\Aggregated\LA.csv"
)
CSVProcessor.count_cells(
    r"data\brokermetrics_data\Aggregated\LA.csv"
)



CSVProcessor.combine_csv_files(
    r'data\brokermetrics_data\Orange County',
    r'data\brokermetrics_data\Aggregated\OC.csv'
)

CSVProcessor.drop_duplicates_in_csv(
    r"data\brokermetrics_data\Aggregated\OC.csv"
)
CSVProcessor.count_cells(
    r"data\brokermetrics_data\Aggregated\OC.csv"
)

CSVProcessor.combine_csv_files(
    r'data\brokermetrics_data\San Bernardino',
    r'data\brokermetrics_data\Aggregated\SB.csv'
)

CSVProcessor.drop_duplicates_in_csv(
    r"data\brokermetrics_data\Aggregated\SB.csv"
)
CSVProcessor.count_cells(
    r"data\brokermetrics_data\Aggregated\SB.csv"
)



# Process the CSV files
CSVProcessor.combine_csv_files(
    r'data\brokermetrics_data\Aggregated',
    r'data\brokermetrics_data\Master\10232024.csv'
)

CSVProcessor.drop_duplicates_in_csv(
    r"data\brokermetrics_data\Master\10232024.csv"
)
CSVProcessor.count_cells(
    r"data\brokermetrics_data\Master\10232024.csv"
)
CSVProcessor.count_cells(
    r"data\brokermetrics_data\Master\10212024.csv"
)

