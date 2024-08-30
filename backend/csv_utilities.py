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
            # Find duplicates based on the specified columns
            duplicates = df[df.duplicated(subset=columns, keep=False)]
            return duplicates
        return None

    @staticmethod
    def remove_duplicates_keep_filled(file_path, output_file_path=None):
        """
        Removes duplicates from the CSV file, keeping the row with more filled columns.
        
        Parameters:
        file_path (str): The path to the input CSV file.
        output_file_path (str, optional): The path to save the cleaned CSV file.
        """
        try:
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            
            # Ensure that we keep all columns for processing
            if df.empty:
                print("The DataFrame is empty.")
                return

            # Create a column to count non-NaN values
            df['non_nan_count'] = df.notna().sum(axis=1)
            
            # Drop duplicates while keeping the row with the maximum 'non_nan_count'
            df_sorted = df.sort_values(by='non_nan_count', ascending=False)
            df_unique = df_sorted.drop_duplicates(subset=['first_name', 'last_name', 'email'], keep='first')
            
            # Drop the temporary 'non_nan_count' column
            df_unique = df_unique.drop(columns=['non_nan_count'])
            
            # Save the cleaned DataFrame
            save_path = output_file_path if output_file_path else file_path
            df_unique.to_csv(save_path, index=False)
            print(f"Cleaned CSV saved successfully at {save_path}")
            
        except Exception as e:
            print(f"An error occurred: {e}")

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

# Example usage
if __name__ == "__main__":
    file_path = r'C:\ReMax\CSV-Tools\agentdata\reverse_prospect_data\reverse_prospect_agent_list.csv'
    output_file_path = r'C:\ReMax\CSV-Tools\agentdata\reverse_prospect_data\reverse_prospect_agent_list.csv'
    
    df = CSVUtilities.load_csv(file_path)
    if df is not None:
        df = CSVUtilities.strip_cells(df)
        df = CSVUtilities.convert_to_lowercase(df)
        df = CSVUtilities.convert_phone_numbers(df, phone_columns=['name']) # adjust column names as needed
        CSVUtilities.save_csv(df, output_file_path)
