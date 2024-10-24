import pandas as pd

def convert_csv_to_lowercase(input_file_path, output_file_path):
    """
    Converts all cells in a CSV file to lowercase and saves the result.
    
    Parameters:
    input_file_path (str): The path to the input CSV file.
    output_file_path (str): The path where the modified CSV file will be saved.
    """
    df = pd.read_csv(input_file_path)
    df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    df.to_csv(output_file_path, index=False)

def load_and_preprocess_data(input_csv_path, master_csv_path):
    """
    Load and preprocess CSV files by converting all text to lowercase.
    
    Args:
    input_csv_path: Path to the input CSV file.
    master_csv_path: Path to the master CSV file.
    
    Returns:
    input_df: Preprocessed input DataFrame.
    master_df: Preprocessed master DataFrame.
    """
    input_df = pd.read_csv(input_csv_path).map(lambda x: x.lower() if isinstance(x, str) else x)
    master_df = pd.read_csv(master_csv_path).map(lambda x: x.lower() if isinstance(x, str) else x)
    return input_df.drop_duplicates(), master_df.drop_duplicates()

def find_missing_rows(main_df, supplementary_df, key_columns):
    """
    Adds rows from supplementary_df to main_df if they are not already present
    based on the key columns (e.g., 'First Name' and 'Last Name').
    
    Args:
    main_df: The DataFrame to which non-matching rows will be added.
    supplementary_df: The DataFrame from which non-matching rows will be sourced.
    key_columns: List of column names to use as keys for comparison.
    
    Returns:
    missing_rows_df: The DataFrame of missing rows that are not present in main_df.
    """
    missing_rows_df = supplementary_df[~supplementary_df[key_columns].apply(tuple, 1).isin(main_df[key_columns].apply(tuple, 1))]
    return missing_rows_df.loc[:, ['First Name', 'Last Name', 'Phone', 'EMail']]

def melt_master_dataframe(master_df):
    """
    Melt the master DataFrame to create a single column for phone numbers.
    
    Args:
    master_df: The master DataFrame.
    
    Returns:
    melted_master_df: The melted DataFrame with phone numbers.
    """
    non_phone_columns = [col for col in master_df.columns if col not in ["Phone 1", "Phone 2", "Phone 3", "Phone 1 Type", "Phone 2 Type", "Phone 3 Type"]]
    melted_master_df = pd.melt(master_df, id_vars=non_phone_columns, value_vars=["Phone 1", "Phone 2", "Phone 3"],
                                var_name="Phone Type", value_name="Phone")
    return melted_master_df

def count_matching_names(main_df, supplementary_df, key_columns):
    """
    Count how many First and Last Name combinations in supplementary_df are in main_df.
    
    Args:
    main_df: The primary DataFrame.
    supplementary_df: The DataFrame to check for matches.
    key_columns: List of column names to use as keys for comparison.
    
    Returns:
    match_count: The number of matching rows based on the key columns.
    """
    merged_df = supplementary_df.merge(main_df, on=key_columns, how='inner')
    return merged_df.shape[0]

def main():
    # Define file paths
    input_csv_path = r"CSV-Tools\backend\agentdata\reverse_prospect_data\reverse_prospect_agent_list.csv"  
    master_csv_path = r"CSV-Tools\backend\brokermetrics_data\Master\10212024.csv"
    

    # Load and preprocess data
    input_df, master_df = load_and_preprocess_data(input_csv_path, master_csv_path)
    print(f'Total rows in input: {len(input_df)}')

    # Count matching names
    match_count = count_matching_names(master_df, input_df, ['First Name', 'Last Name'])
    
    # Melt the master DataFrame
    melted_master_df = melt_master_dataframe(master_df)

    # Find missing rows
    missing_agents = find_missing_rows(melted_master_df, input_df, ['First Name', 'Last Name'])
    missing_agents = missing_agents[missing_agents['Phone'].isnull()]

    # Merge DataFrames
    merged_df = pd.merge(input_df, melted_master_df, on=['First Name', 'Last Name'], how='left', suffixes=('', '_data'))
    merged_df['Phone'] = merged_df['Phone'].fillna(merged_df['Phone_data'])
    merged_df.drop(columns=['Phone_data'], inplace=True)
    merged_df = merged_df[merged_df['Phone'].notna()]

    # Save outputs
    merged_df.to_csv('merged_output.csv', index=False)
    unique_agents = merged_df.drop_duplicates(subset=['First Name', 'Last Name'])
    unique_agents.to_csv("unique_agents.csv", index=False)

    print(f'Match Rate: {len(unique_agents) / len(input_df) * 100:.2f}%')
    print(f'Total agents matched: {len(unique_agents)}')
    print(f'Total agents unmatched: {len(missing_agents)}')
    print(f'Total Phone Numbers Found: {len(merged_df)}')

    output_file_path = "cleaned_merged_output.csv"  
    merged_df.to_csv(output_file_path, index=False)

if __name__ == "__main__":
    main()
