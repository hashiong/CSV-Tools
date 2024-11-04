import pandas as pd
import re

def process_csv(file_path):
    # Load the CSV file
    df = pd.read_csv(file_path)
    
    # Check and process 'Member Email Office Name Member Direct Phone' column
    if 'Member Email Office Name Member Direct Phone' in df.columns:
        df[['EMail', 'Office Name', 'Phone']] = df['Member Email Office Name Member Direct Phone'].apply(split_member_info)
        df.drop(columns=['Member Email Office Name Member Direct Phone'], inplace=True)

    # Check and process 'Agent' column
    if 'Agent' in df.columns:
        df[['First Name', 'Last Name']] = df['Agent'].apply(split_name)
        df.drop(columns=['Agent'], inplace=True)
    
    # Keep only the specified columns
    columns_to_keep = ['EMail', 'Office Name', 'Phone', 'First Name', 'Last Name']
    df = df[columns_to_keep]

    return df

def split_member_info(row):
    # Regex to identify email and phone patterns
    email_match = re.search(r'\S+@\S+', row)
    phone_match = re.search(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', row)
    
    # Extract email and phone if they exist
    email = email_match.group(0) if email_match else ""
    phone = phone_match.group(0) if phone_match else ""
    
    # The rest is assumed to be the office name
    office_name = row.replace(email, "").replace(phone, "").strip()
    
    return pd.Series([email, office_name, phone])

def split_name(full_name):
    # Split the full name by spaces
    name_parts = full_name.split()
    
    # Use the last part as the last name, and the rest as the first name
    last_name = name_parts[-1] if len(name_parts) > 0 else ""
    first_name = " ".join(name_parts[:-1]) if len(name_parts) > 1 else ""
    
    return pd.Series([first_name, last_name])
