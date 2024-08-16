import pandas as pd
import os



def load_csv_columns(file_path):
    try:
        df = pd.read_csv(file_path)
        return set(df.columns)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return set()

def compare_columns_in_folder(folder_path):
    all_columns = {}
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return
    
    # Load columns from each CSV file
    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        columns = load_csv_columns(file_path)
        all_columns[file_path] = columns
    
    if not all_columns:
        print("No valid CSV files found or all files could not be read.")
        return
    
    # Find common and unique columns
    common_cols = set.intersection(*all_columns.values())
    unique_cols = {file: cols - common_cols for file, cols in all_columns.items()}
    
    # Print results
    print("Common Columns:")
    print(common_cols)
    print()
    
    for file, unique in unique_cols.items():
        print(f"Unique to {os.path.basename(file)}:")
        print(unique)
        print()

# Path to the folder containing CSV files.
folder_path = 'data'  # Update this to your folder path

# compare_columns_in_folder(folder_path)
commonCols = ['#', 'Agent ID', 'First Name', 'Last Name', 'EMail', 'Phone 1 Type', 'Phone 1', 'Phone 2 Type', 'Phone 2', 'Phone 3 Type', 'Phone 3', 'Alt. Address', 'Alt. City', 'Alt. Zip', 'Office ID', 'Office Name', 'Office Address', 'Office City', 'Office County' 'Office Zip'  ]

def combine_csv_files(folder_path, common_cols, output_file):
    all_data = []
    
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return
    
    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        try:
            df = pd.read_csv(file_path)
            # Filter columns to include only the common columns
            df_filtered = df[common_cols]
            all_data.append(df_filtered)
        except KeyError as e:
            print(f"Error: {e} - Check if all columns are present in {file_name}")
        except Exception as e:
            print(f"Error loading {file_name}: {e}")
    
    if not all_data:
        print("No data could be combined.")
        return
    
    # Concatenate all filtered DataFrames
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined file saved as {output_file}")

# Define the path to your folder containing CSV files
folder_path = 'data'  # Update this to your folder path

# Define the common columns to include
common_cols = ['#', 'Agent ID', 'First Name', 'Last Name', 'EMail', 
                'Phone 1 Type', 'Phone 1', 'Phone 2 Type', 'Phone 2', 
                'Phone 3 Type', 'Phone 3', 'Alt. Address', 'Alt. City', 
                'Alt. Zip', 'Office ID', 'Office Name', 'Office Address', 
                'Office City', 'Office County', 'Office Zip']

# Define the path for the output combined CSV file
output_file = 'combined-gent-list.csv'

# Combine CSV files
combine_csv_files(folder_path, common_cols, output_file)
