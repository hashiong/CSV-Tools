import pandas as pd

class CSVUtilities:
    
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
            return pd.read_csv(file_path)
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

    @staticmethod
    def strip_cells(df):
        """
        Strips leading and trailing spaces from all cells in the DataFrame.

        Parameters:
        df (pd.DataFrame): The DataFrame to process.

        Returns:
        pd.DataFrame: DataFrame with stripped cells.
        """
        return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    @staticmethod
    def convert_to_lowercase(df):
        """
        Converts all string values in the DataFrame to lowercase.

        Parameters:
        df (pd.DataFrame): The DataFrame to process.

        Returns:
        pd.DataFrame: DataFrame with all string values converted to lowercase.
        """
        return df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    
    @staticmethod
    def convert_phone_numbers(df, phone_columns):
        """
        Converts phone numbers to pure digits in specified columns.

        Parameters:
        df (pd.DataFrame): The DataFrame to process.
        phone_columns (list): List of columns containing phone numbers.

        Returns:
        pd.DataFrame: DataFrame with phone numbers converted to pure digits.
        """
        for column in phone_columns:
            if column in df.columns:
                df[column] = df[column].astype(str).str.replace(r'\D', '', regex=True)
        return df

    @staticmethod
    def find_duplicates(file_path, columns):
        """
        Finds duplicate rows in a CSV file based on specified columns.

        Parameters:
        file_path (str): The path to the CSV file.
        columns (list): A list of column names to check for duplicates.

        Returns:
        pd.DataFrame: DataFrame containing duplicate rows.
        """
        df = CSVUtilities.load_csv(file_path)
        if df is not None:
            return df[df.duplicated(subset=columns, keep=False)]
        return None

    @staticmethod
    def remove_duplicates_keep_filled(file_path, output_file_path=None):
        """
        Removes duplicates from the CSV file, keeping the row with more filled columns.

        Parameters:
        file_path (str): The path to the input CSV file.
        output_file_path (str, optional): The path to save the cleaned CSV file.
        """
        df = CSVUtilities.load_csv(file_path)
        if df is not None and not df.empty:
            df['non_nan_count'] = df.notna().sum(axis=1)
            df = df.sort_values(by='non_nan_count', ascending=False).drop_duplicates(subset=['first_name', 'last_name', 'email'], keep='first')
            df = df.drop(columns=['non_nan_count'])
            CSVUtilities.save_csv(df, output_file_path or file_path)
        else:
            print("The DataFrame is empty or could not be loaded.")

    @staticmethod
    def combine_and_remove_duplicates(csv_file1, csv_file2, output_file, subset_columns=None):
        """
        Combine two CSV files and remove duplicates.

        Parameters:
        csv_file1 (str): Path to the first CSV file.
        csv_file2 (str): Path to the second CSV file.
        output_file (str): Path where the resulting CSV file will be saved.
        subset_columns (list, optional): List of columns to consider for identifying duplicates.
        """
        try:
            df1 = CSVUtilities.load_csv(csv_file1)
            df2 = CSVUtilities.load_csv(csv_file2)

            if df1 is not None and df2 is not None:
                combined_df = pd.concat([df1, df2], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=subset_columns)
                CSVUtilities.save_csv(combined_df, output_file)
            else:
                print("One or both of the DataFrames could not be loaded.")
        except Exception as e:
            print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Example usage of methods
    file_path = r'backend\agentdata\new_data\new_added_data.csv'
    output_file_path = r'backend\agentdata\new_data\cleaned_data.csv'
    
    df = CSVUtilities.load_csv(file_path)
    if df is not None:
        df = CSVUtilities.strip_cells(df)
        df = CSVUtilities.convert_to_lowercase(df)
        df = CSVUtilities.convert_phone_numbers(df, phone_columns=['phone']) # adjust column names as needed
        CSVUtilities.save_csv(df, output_file_path)

    CSVUtilities.combine_and_remove_duplicates(
        r'backend\agentdata\aggregate_data\09012024.csv',
        r'backend\agentdata\aggregate_data\09042024.csv',
        r'backend\agentdata\aggregate_data\master_data.csv'
    )
