import pandas as pd
import os, re

class CSVmodifieir:

    def __init__(self, filename) -> None:
        self.filename = filename
        self.df = None  # Placeholder for the DataFrame

        # Attempt to load the CSV file into a DataFrame
        try:
            self.df = pd.read_csv(self.filename)
            print(f"Successfully loaded {self.filename}.")
        except FileNotFoundError:
            print(f"Error: The file {self.filename} does not exist.")
        except Exception as e:
            print(f"Error loading {self.filename}: {e}")
    
    def add_column(self, column_name, default_value):
        """
        Adds a new column to the DataFrame with a specified default value.

        Args:
            column_name (str): The name of the new column to add.
            default_value: The default value to fill in the new column.
        """
        if self.df is not None:
            if column_name in self.df.columns:
                print(f"Column '{column_name}' already exists.")
            else:
                self.df[column_name] = default_value
                print(f"Column '{column_name}' added with default value '{default_value}'.")
        else:
            print("Error: DataFrame is not loaded.")
    
    def clean_phone_cols(self, phone_cols):
        if self.df is not None:
            for col in phone_cols:
                if(col in self.df.columns):
                    def clean_phone(phone):
                        if pd.isna(phone):
                            return ''
                        # Remove anything that's not a digit
                        cleaned_phone = re.sub(r'\D', '', str(phone))
                        return cleaned_phone if len(cleaned_phone) == 10 else ''
                    # Apply the cleaning function to the column
                    self.df[col] = self.df[col].apply(clean_phone)
        return


    
    def remove_columns(self, keep_cols_list):
        """
        Keeps only the specified columns in the DataFrame and removes all others.

        Args:
            keep_cols_list (list): A list of column names to keep.
        """
        if self.df is not None:
            existing_cols = set(self.df.columns)
            keep_cols_set = set(keep_cols_list)
            
            # Identify columns that will be kept
            cols_to_keep = existing_cols.intersection(keep_cols_set)
            
            if cols_to_keep:
                self.df = self.df[list(cols_to_keep)]
                print(f"Kept columns: {', '.join(cols_to_keep)}.")
            else:
                print("Error: No matching columns to keep.")
        else:
            print("Error: DataFrame is not loaded.")

    def save_csv(self, output_filename=None):
        """
        Saves the modified DataFrame back to a CSV file.

        Args:
            output_filename (str, optional): The name of the output CSV file. 
                                            If not provided, it will overwrite the original file.
        """
        if self.df is not None:
            save_path = output_filename if output_filename else self.filename
            self.df.to_csv(save_path, index=False)
            print(f"DataFrame saved to '{save_path}'.")
        else:
            print("Error: DataFrame is not loaded.")

instance = CSVmodifieir("cleaned_data.csv")
info_needed = ["First Name","Last Name", "EMail", "Phone 1", "Phone 2", "Phone 3", "Alt. City", "Alt. Zip", "Office Zip","Country"]
phone_cols = ['Phone 1', 'Phone 2', 'Phone 3']
instance.clean_phone_cols(phone_cols)

instance.save_csv("cleaned_data_for_sql.csv")