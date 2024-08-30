import pandas as pd
import numpy as np

class CSVstandarizer:

    @staticmethod
    def load_csv(file_path):
        """
        Loads a CSV file into a DataFrame.
        
        Parameters:
        file_path (str): The path to the CSV file.
        
        Returns:
        pd.DataFrame: The loaded DataFrame, or None if an error occurred.
        """
        try:
            df = pd.read_csv(file_path)
            return df
        except FileNotFoundError:
            print(f"Error: The file at {file_path} was not found.")
        except pd.errors.EmptyDataError:
            print("Error: The file is empty.")
        except pd.errors.ParserError:
            print("Error: There was a parsing error. Check the file's format.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return None

    @staticmethod
    def clean_column_names(file_path, output_file_path=None):
        """
        Cleans column names by removing leading/trailing spaces, converting to lowercase, 
        and replacing spaces with underscores.
        
        Parameters:
        file_path (str): The path to the CSV file.
        output_file_path (str, optional): The path to save the cleaned CSV file.
        """
        df = CSVstandarizer.load_csv(file_path)
        if df is not None:
            df.columns = df.columns.str.strip().str.lower().str.replace('.','').str.replace(' ', '_')
            if output_file_path:
                save_path = output_file_path
            else:
                save_path = file_path
            CSVstandarizer.save_csv(df, save_path)

    @staticmethod
    def validate_and_clean_csv(file_path, output_file_path=None):
        """
        Validates the data in the CSV file according to predefined rules and cleans invalid entries.
        
        Parameters:
        file_path (str): The path to the CSV file.
        output_file_path (str, optional): The path to save the cleaned CSV file.
        """
        validation_rules = {
            '#': r'^[\w\s]+$',
            'agent_id': r'^[\w\s]+$',
            'first_name': r'^[\w\s]+$',
            'last_name': r'^[\w\s]+$',
            'office_id': r'^[\w\s]+$',
            'office_name': r'^[\w\s]+$',
            'office_address': r'^[\w\s]+$',
            'office_city': r'^[\w\s]+$',
            'office_zip': r'^[1-9]\d{4}$',
            'office_county': r'^[\w\s]+$',
            'phone_1': r'^[1-9]\d{9}$',
            'phone_1_type': r'^[\w\s]+$',
            'phone_2': r'^[1-9]\d{9}$',
            'phone_2_type': r'^[\w\s]+$',
            'phone_3': r'^[1-9]\d{9}$',
            'phone_3_type': r'^[\w\s]+$',
            'email': r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$',
            'alt_address': r'^[\w\s]+$',
            'alt_city': r'^[\w\s]+$',
            'alt_zip': r'^[1-9]\d{4}$'
        }

        df = CSVstandarizer.load_csv(file_path)
        if df is not None:
            columns_to_keep = [col for col in df.columns if col in validation_rules]
            df = df[columns_to_keep]
            for col, pattern in validation_rules.items():
                if col in df.columns:
                    invalid_mask = ~df[col].astype(str).str.match(pattern)
                    df.loc[invalid_mask, col] = np.nan  # Set invalid cells to NaN
            if output_file_path:
                save_path = output_file_path
            else:
                save_path = file_path
            CSVstandarizer.save_csv(df, save_path)

    @staticmethod
    def convert_csv_to_lowercase(file_path, output_file_path=None):
        """
        Converts all string entries in the CSV file to lowercase.
        
        Parameters:
        file_path (str): The path to the CSV file.
        output_file_path (str, optional): The path to save the converted CSV file.
        """
        df = CSVstandarizer.load_csv(file_path)
        if df is not None:
            df = df.map(lambda s: s.strip().lower() if isinstance(s, str) else s)
            if output_file_path:
                save_path = output_file_path
            else:
                save_path = file_path
            CSVstandarizer.save_csv(df, save_path)

    def add_col_to_csv(file_path, new_col_name, output_file_path=None, default_value = None):
        df = CSVstandarizer.load_csv(file_path)
        df[new_col_name] = default_value
         # Determine the output file path
        if output_file_path is None:
            output_file_path = file_path
        CSVstandarizer.save_csv(df, output_file_path)

    @staticmethod
    def save_csv(data_frame, file_path, index=False):
        """
        Saves a pandas DataFrame to a CSV file.
        
        Parameters:
        data_frame (pd.DataFrame): The DataFrame to save.
        file_path (str): The path where the CSV file will be saved.
        index (bool): Whether to write row names (index) to the file. Default is False.
        
        Returns:
        bool: True if the file was saved successfully, False otherwise.
        """
        try:
            data_frame.to_csv(file_path, index=index)
            print(f"File saved successfully at {file_path}")
            return True
        except Exception as e:
            print(f"An error occurred while saving the file: {e}")
            return False


