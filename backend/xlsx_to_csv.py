import os
import pandas as pd

# Define the folder containing the Excel files
folder_path = r'CSV-Tools\backend\agentdata\mls_data'  # Adjust the folder path as needed

# Iterate through all files in the folder
for filename in os.listdir(folder_path):
    # Check if the file is an Excel file (ends with .xlsx or .xls)
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        # Construct the full file path
        file_path = os.path.join(folder_path, filename)
        
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        # Create an output CSV file path by changing the file extension to .csv
        output_filename = filename.rsplit('.', 1)[0] + '.csv'
        output_path = os.path.join(folder_path, output_filename)
        
        # Write the DataFrame to a CSV file
        df.to_csv(output_path, index=False)
        
        print(f"Processed {filename} and saved as {output_filename}")

print("All Excel files have been processed and converted to CSV.")
