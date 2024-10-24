import pandas as pd

def convert_csv_to_lowercase(input_file_path, output_file_path):
    """
    Converts all cells in a CSV file to lowercase and saves the result.
    """
    df = pd.read_csv(input_file_path)
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    df.to_csv(output_file_path, index=False)

def load_and_preprocess_data(input_csv_path, master_csv_path):
    """
    Load and preprocess CSV files by converting all text to lowercase.
    """
    input_df = pd.read_csv(input_csv_path).applymap(lambda x: x.lower() if isinstance(x, str) else x)
    master_df = pd.read_csv(master_csv_path).applymap(lambda x: x.lower() if isinstance(x, str) else x)
    return input_df.drop_duplicates(), master_df.drop_duplicates()

def find_missing_rows(main_df, supplementary_df, key_columns):
    """
    Find rows in supplementary_df that are missing in main_df based on key columns.
    """
    missing_rows_df = supplementary_df[~supplementary_df[key_columns].apply(tuple, 1).isin(main_df[key_columns].apply(tuple, 1))]
    return missing_rows_df.loc[:, ['First Name', 'Last Name', 'Phone', 'EMail']]

def melt_master_dataframe(master_df):
    """
    Melt the master DataFrame to handle multiple phone numbers.
    """
    non_phone_columns = [col for col in master_df.columns if col not in ["Phone 1", "Phone 2", "Phone 3", "Phone 1 Type", "Phone 2 Type", "Phone 3 Type"]]
    melted_master_df = pd.melt(master_df, id_vars=non_phone_columns, value_vars=["Phone 1", "Phone 2", "Phone 3"],
                               var_name="Phone Type", value_name="Phone")
    return melted_master_df

def count_matching_names(main_df, supplementary_df, key_columns):
    """
    Count how many First and Last Name combinations in supplementary_df match those in main_df.
    """
    merged_df = supplementary_df.merge(main_df, on=key_columns, how='inner')
    return merged_df.shape[0]

