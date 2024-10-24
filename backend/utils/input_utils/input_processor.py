import os
import pandas as pd
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
        logging.error("Error: There was a parsing error. Check the file's format.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None


def clean_column_names(df):
    """Cleans column names by removing leading/trailing spaces, converting to lowercase, and replacing spaces with underscores."""
    df.columns = df.columns.str.strip().str.lower().str.replace('.', '').str.replace(' ', '_')
    return df


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
    return df


def validate_and_clean_csv(df, validation_rules):
    """Validates the data in the DataFrame according to predefined rules and cleans invalid entries."""
    columns_to_keep = [col for col in df.columns if col in validation_rules]
    df = df[columns_to_keep]
    for col, pattern in validation_rules.items():
        if col in df.columns:
            invalid_mask = ~df[col].astype(str).str.match(pattern)
            df.loc[invalid_mask, col] = np.nan  # Set invalid cells to NaN
    return df


def convert_csv_to_lowercase(df):
    """Converts all string entries in the DataFrame to lowercase."""
    return df.applymap(lambda s: s.strip().lower() if isinstance(s, str) else s)


def save_csv(df, file_path, index=False):
    """Saves a pandas DataFrame to a CSV file."""
    try:
        df.to_csv(file_path, index=index)
        logging.info(f"File saved successfully at {file_path}")
    except Exception as e:
        logging.error(f"An error occurred while saving the file: {e}")


def clean_phone_numbers(df, phone_columns):
    """Cleans phone numbers in the specified columns of a CSV file."""
    if df is not None:
        df = convert_phone_numbers(df, phone_columns)
    return df


def drop_duplicates_in_csv(file_path):
    """Drops duplicate rows from a CSV file."""
    df = load_csv(file_path)
    if df is not None:
        row_count_before = len(df)
        logging.info(f'Row count before dropping duplicates: {row_count_before}')
        df_cleaned = df.drop_duplicates()
        row_count_after = len(df_cleaned)
        logging.info(f'Row count after dropping duplicates: {row_count_after}')
        save_csv(df_cleaned, file_path)


def check_columns(df, cols):
    """Checks if specified columns are present in the DataFrame."""
    if df is not None:
        for col in cols:
            if col not in df.columns:
                return False
    return True
