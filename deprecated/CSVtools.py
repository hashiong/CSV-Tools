import pandas as pd
import os, re

class CSVtools:

    @classmethod
    def load_csv(cls, filename):
        """Loads the CSV file into a DataFrame."""
        try:
            df = pd.read_csv(filename)
            print(f"Successfully loaded {filename}.")
            return df
        except FileNotFoundError:
            print(f"Error: The file {filename} does not exist.")
            return None
        except Exception as e:
            print(f"Error loading {filename}: {e}")
            return None
        
    @classmethod
    def change_column_name(cls, df, column_name, new_name):
        """Change column name."""
        if df is not None:
            if column_name in df.columns:
                df = df.rename(columns={column_name: new_name})
                print(f"Column '{column_name}' successfully changed to '{new_name}'.")
                return df
            else:
                print(f"Error: Column '{column_name}' does not exist.")
        else:
            print("Error: DataFrame is not loaded.")
        return df
        

    @classmethod
    def add_column(cls, df, column_name, default_value):
        """
        Adds a new column to the DataFrame with a specified default value.
        """
        if df is not None:
            if column_name in df.columns:
                print(f"Column '{column_name}' already exists.")
            else:
                df[column_name] = default_value
                print(f"Column '{column_name}' added with default value '{default_value}'.")
        else:
            print("Error: DataFrame is not loaded.")

    @classmethod
    def clean_phone_cols(cls, df, phone_cols):
        """Cleans phone number columns by removing non-digit characters and keeping only valid 10-digit numbers."""
        if df is not None:
            for col in phone_cols:
                if col in df.columns:
                    df[col] = df[col].apply(cls.clean_phone)
        else:
            print("Error: DataFrame is not loaded.")

    @classmethod
    def clean_phone(cls, phone):
        """Helper function to clean phone numbers."""
        if pd.isna(phone):
            return ''
        cleaned_phone = re.sub(r'\D', '', str(phone))
        return cleaned_phone if len(cleaned_phone) == 10 else ''

    @classmethod
    def remove_columns(cls, df, keep_cols_list):
        """
        Keeps only the specified columns in the DataFrame and removes all others.
        """
        if df is not None:
            existing_cols = set(df.columns)
            keep_cols_set = set(keep_cols_list)

            # Identify columns that will be kept
            cols_to_keep = existing_cols.intersection(keep_cols_set)

            if cols_to_keep:
                df = df[list(cols_to_keep)]
                print(f"Kept columns: {', '.join(cols_to_keep)}.")
                return df
            else:
                print("Error: No matching columns to keep.")
                return df
        else:
            print("Error: DataFrame is not loaded.")
            return df
        
    @classmethod
    def convert_to_lowercase(cls, df):
        """Convert all values in the dataframe to lowercase except the column names."""
        # Convert all values to lowercase
        df = df.map(lambda x: x.strip().lower() if isinstance(x, str) else x)
        return df

    @classmethod
    def process_csv(cls, file_path, output_path):
        """Read a CSV file, convert its values to lowercase, and save the result."""
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Convert values to lowercase
        df = cls.convert_to_lowercase(df)
        
        # Save the modified dataframe to a new CSV file
        df.to_csv(output_path, index=False)

    @classmethod
    def save_csv(cls, df, output_filename):
        """
        Saves the modified DataFrame back to a CSV file.
        """
        if df is not None:
            save_path = output_filename 
            df.to_csv(save_path, index=False)
            print(f"DataFrame saved to '{save_path}'.")
        else:
            print("Error: DataFrame is not loaded.")


