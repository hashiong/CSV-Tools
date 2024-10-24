import pandas as pd

def count_rows_with_phone_values(df, phone_columns):
    """
    Counts the number of rows in the DataFrame that contain any value in the specified phone columns.

    Parameters:
    df (pd.DataFrame): The DataFrame to analyze.
    phone_columns (list): A list of column names to check.

    Returns:
    int: The count of rows containing any value in the specified columns.
    """
    # Check if columns exist in the DataFrame
    existing_columns = [col for col in phone_columns if col in df.columns]

    # Count rows where any specified phone column has a non-null, non-empty value
    count_with_values = df[existing_columns].notnull().any(axis=1).sum()
    total_rows = len(df)

    # Print stats
    print(f'Total rows: {total_rows}')
    print(f'Number of rows with phone values: {count_with_values}')

    return count_with_values

# Example usage
# Load your CSV file into a DataFrame (replace with your file)
df = pd.read_csv(r'CSV-Tools\backend\brokermetrics_data\Aggregated\SB.csv')


# Check the data types of all columns
print(df.dtypes)
# Input list of phone columns
phone_columns = ["Phone 1", "Phone 2", "Phone 3"]

# Count the rows with any phone values and print stats
row_count = count_rows_with_phone_values(df, phone_columns)
