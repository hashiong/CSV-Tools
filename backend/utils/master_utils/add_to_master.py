import pandas as pd

def update_master_csv():
    # File paths
    NEW_CSV_PATH = r"backend\data\brokermetrics_data\Master\new.csv"
    MASTER_CSV_PATH = r"backend\data\brokermetrics_data\Master\10232024.csv"
    UPDATED_MASTER_CSV_PATH = r"backend\data\brokermetrics_data\Master\11132024.csv"

    # Define the expected columns
    EXPECTED_COLUMNS = [
        "Agent ID",
        "First Name",
        "Last Name",
        "Office ID",
        "Office Name",
        "Phone 1",
        "Phone 1 Type",
        "Phone 2",
        "Phone 2 Type",
        "Phone 3",
        "Phone 3 Type",
        "EMail",
        "Alt. Address",
        "Alt. City",
        "Alt. Zip"
    ]

    # Load the new data and the master data
    try:
        new_data = pd.read_csv(NEW_CSV_PATH)
        master_data = pd.read_csv(MASTER_CSV_PATH)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return

    # Ensure both datasets contain the expected columns and order them
    try:
        new_data = new_data[EXPECTED_COLUMNS]
        master_data = master_data[EXPECTED_COLUMNS]
    except KeyError as e:
        print(f"Error: Missing columns in one of the CSV files - {e}")
        return

    # Append the new data to the master data
    updated_master_data = pd.concat([master_data, new_data], ignore_index=True)

    # Remove rows where all phone number fields are empty (NaN)
    updated_master_data = updated_master_data.dropna(
        subset=["Phone 1", "Phone 2", "Phone 3"], how="all"
    )

    # Save the updated master file
    try:
        updated_master_data.to_csv(UPDATED_MASTER_CSV_PATH, index=False)
        print(f"Updated master file saved successfully to {UPDATED_MASTER_CSV_PATH}")
    except Exception as e:
        print(f"Error saving the updated master CSV file: {e}")


# Run the function only if the script is executed directly
if __name__ == "__main__":
    update_master_csv()
