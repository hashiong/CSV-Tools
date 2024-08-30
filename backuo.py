import pandas as pd
from csv_utilities import CSVUtilities

class CSVProcessor:

    valid_data_cols = ["first_name", "last_name", "email", "office_name"]

    @staticmethod
    def validate_csv(df, valid_cols):
        """Validate if the dataframe contains all valid columns."""
        if df is not None:
            cols = df.columns
            for c in valid_cols:
                if c not in cols:
                    return False
        return True

    @staticmethod
    def capitalize_columns(df):
        df.columns = [col.capitalize() for col in df.columns]
        return df

    @staticmethod
    def capitalize_values(df, columns):
        for col in columns:
            if col in df.columns:
                df[col] = df[col].str.capitalize()
        return df

    @staticmethod
    def cross_match(input_file, reference_file, matching_cols):
        # match by name, or agent_id, 
        """Cross match the columns of each row of each df and return the list of matched columns from the reference dataframe."""
        """Primary match based on name, agent_id, or phone"""
        input_df = CSVUtilities.load_csv(input_file)
        reference_df = CSVUtilities.load_csv(reference_file)
        # Validate the needed columns for matching
        if (input_df is not None and reference_df is not None and 
            CSVProcessor.validate_csv(input_df, CSVProcessor.valid_data_cols) and 
            CSVProcessor.validate_csv(reference_df, CSVProcessor.valid_data_cols)):

            if "phone" in matching_cols:
                # Remove 'phone' from matching_cols and handle separately
                non_phone_cols = [col for col in matching_cols if col != "phone"]
                
                # Reshape reference_df to have one phone column
                phone_matches_input = pd.melt(reference_df, 
                                            id_vars=non_phone_cols, 
                                            value_vars=["phone_1", "phone_2", "phone_3"], 
                                            var_name="phone_type", 
                                            value_name="phone")
            
                print(phone_matches_input)

            # Match based on names
            name_matches = pd.merge(reference_df, input_df, on=["first_name", "last_name"], how="inner")

            # Match based on agent_id
            agentid_matches = pd.merge(reference_df, input_df, on=["agent_id"], how="inner")
            
            # Match based on office id
            office_id_matches = pd.merge(input_df, reference_df, on=["office_id"], how="inner")

            # Match based on email
            email_matches = pd.merge(input_df, reference_df, on=["email"], how="inner")

            # Combine all matches
            # all_matches = pd.concat([name_matches, phone_matches, email_matches]).drop_duplicates()

            # Drop unnecessary or redundant columns
            all_matches = all_matches.drop(columns=[col for col in all_matches.columns if col.endswith('_x') or col.endswith('_y') or col.endswith('_ref')])

            # Drop any remaining duplicates
            all_matches = all_matches.loc[:, ~all_matches.columns.duplicated()]

            all_matches = all_matches.drop(columns="phone_type")

            # Capitalize column names
            all_matches = CSVProcessor.capitalize_columns(all_matches)

            # Capitalize values in specified columns
            all_matches = CSVProcessor.capitalize_values(all_matches, ["First_name", "Last_name", "Office_city", "Office_location"])

            # Resolve duplicates by keeping the row with the most non-null columns
            all_matches['non_null_count'] = all_matches.notnull().sum(axis=1)
            all_matches = all_matches.sort_values('non_null_count', ascending=False).drop_duplicates(subset=['First_name', 'Last_name'], keep='first')
            all_matches = all_matches.drop(columns='non_null_count')

            return all_matches
        
        return pd.DataFrame()

# Assuming valid_data_cols and valid_reference_cols are defined as in your code
input_file = r"C:/ReMax/CSV-Tools/agentdata/aggregate_data/cleaned_combined_data.csv"
reference_file = r"C:/ReMax/CSV-Tools/agentdata/reverse_prospect_data/reverse_prospect_list.csv"

valid_data_cols = ["first_name", "last_name", "email", "office_name", "phone"]

matched_df = CSVProcessor.cross_match(reference_file,input_file, ["phone"])
matched_df.to_csv("matched_data.csv", index=False)
