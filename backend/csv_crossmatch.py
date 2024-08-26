import pandas as pd
from backend.csv_utilities import CSVUtilities

class CSVProcessor:
    # Define valid columns
    valid_reference_cols = ["first_name", "last_name", "email", "phone_1", "phone_2", "phone_3", "office_name", "country"]
    valid_input_cols = ["first_name", "last_name", "email", "office_name", "phone"]

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
    def cross_match(input_file, reference_file):
        """Cross match the columns of each row of each df and return the list of matched columns from the reference dataframe."""
        input_df = CSVUtilities.load_csv(input_file)
        reference_df = CSVUtilities.load_csv(reference_file)

        # Validate the needed columns for matching
        if (input_df is not None and reference_df is not None and 
            CSVProcessor.validate_csv(input_df, CSVProcessor.valid_input_cols) and 
            CSVProcessor.validate_csv(reference_df, CSVProcessor.valid_reference_cols)):

            # Match based on names
            name_matches = pd.merge(input_df, reference_df, on=["first_name", "last_name"], how="inner")

            # Match based on phones
            phone_matches_input = pd.melt(input_df, id_vars=["first_name", "last_name"], value_vars=["phone_1", "phone_2", "phone_3"], var_name="phone_type", value_name="phone")
            phone_matches_ref = reference_df[["first_name", "last_name", "phone"]].copy()
            phone_matches_ref.rename(columns={"phone": "phone_ref"}, inplace=True)

            phone_matches = pd.merge(phone_matches_input, phone_matches_ref, left_on=["first_name", "last_name", "phone"], right_on=["first_name", "last_name", "phone_ref"], how="inner")
            
            # Match based on email
            email_matches = pd.merge(input_df, reference_df, on=["email"], how="inner")

            # Combine all matches
            all_matches = pd.concat([name_matches, phone_matches, email_matches]).drop_duplicates()

            # Drop unnecessary or redundant columns
            all_matches = all_matches.drop(columns=[col for col in all_matches.columns if col.endswith('_x') or col.endswith('_y') or col.endswith('_ref')])

            # Drop any remaining duplicates
            all_matches = all_matches.loc[:, ~all_matches.columns.duplicated()]

            all_matches = all_matches.drop(columns="phone_type")

            # Capitalize column names
            all_matches = CSVProcessor.capitalize_columns(all_matches)

            # Capitalize values in specified columns
            all_matches = CSVProcessor.capitalize_values(all_matches, ["First_name", "Last_name", "Office_city", "Office_location"])

            return all_matches
        
        return pd.DataFrame()
