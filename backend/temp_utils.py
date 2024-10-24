import csv
import re

# Input and output file names
input_file = r'backend\agentdata\reverse_prospect_data\vickyreverse.csv'
output_file = r'backend\agentdata\reverse_prospect_data\vicky_reverse_prospect.csv'

# Read the CSV file
with open(input_file, 'r', newline='') as csvfile_in:
    reader = csv.reader(csvfile_in)
    data = list(reader)

# Convert column headers to lower case
data[0] = [col.lower() for col in data[0]]

# Get the index of the 'phone' column
try:
    phone_index = data[0].index('phone')
except ValueError:
    print("No 'phone' column found in the input CSV file.")
    phone_index = None

# Process the data rows
for row in data[1:]:
    # Convert all cells to lower case
    for i in range(len(row)):
        row[i] = row[i].lower()
    
    # Process 'phone' column
    if phone_index is not None:
        phone_number = row[phone_index].strip()
        # Check if phone number is in xxxxxxxxxx format
        if re.fullmatch(r'\d{10}', phone_number):
            pass  # Already in correct format
        # Check if phone number is in xxx-xxx-xxxx format
        elif re.fullmatch(r'\d{3}-\d{3}-\d{4}', phone_number):
            # Remove the dashes
            row[phone_index] = phone_number.replace('-', '')
        else:
            # Make the cell empty
            row[phone_index] = ''

# Write the updated data to the output CSV file
with open(output_file, 'w', newline='') as csvfile_out:
    writer = csv.writer(csvfile_out)
    writer.writerows(data)
