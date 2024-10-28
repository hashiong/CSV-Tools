import re
import pandas as pd

# Function to format phone number
def format_phone_number(phone):
    # Remove all non-numeric characters using regex
    phone = re.sub(r'\D', '', str(phone))
    return phone if len(phone) == 10 else None  # Return only if it's a valid 10-digit phone number

# Function to clean the CSV for texting purposes
def clean_csv_for_texting(file_path):
    df = pd.read_csv(file_path)
    split_rows = []

    # Split each row by phone numbers
    for _, row in df.iterrows():
        first_name = row["First Name"]
        last_name = row["Last Name"]
        office = row["Office Name"]
        email = row["EMail"]
        phones = [row.get("Phone 1"), row.get("Phone 2"), row.get("Phone 3")]

        # Create a new row for each formatted phone number
        for phone in phones:
            if pd.notna(phone):
                formatted_phone = format_phone_number(phone)
                if formatted_phone:
                    split_rows.append({
                        'first name': first_name,
                        'last name': last_name,
                        'office': office,
                        'phone': formatted_phone,
                        'email': email
                    })

    # Create a new DataFrame with the split rows
    new_df = pd.DataFrame(split_rows)

    # Remove duplicate phone numbers
    new_df = new_df.drop_duplicates(subset='phone')

    # Remove duplicate emails (keep email only for the first occurrence)
    duplicate_emails = new_df['email'].duplicated(keep=False)
    new_df.loc[duplicate_emails, 'email'] = ''
    
    return new_df

    
