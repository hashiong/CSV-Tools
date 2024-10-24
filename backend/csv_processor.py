import pandas as pd
from csv_utilities import CSVUtilities

class CSVProcessor:

    valid_input_data_cols = ["first_name", "last_name", "email", "office_name"]
    valid_ref_data_cols = [
        "#", "agent_id", "first_name", "last_name", "office_id", "office_name",
        "office_address", "office_city", "office_zip", "office_county", "phone_1",
        "phone_1_type", "phone_2", "phone_2_type", "phone_3", "phone_3_type",
        "email", "alt_address", "alt_city", "alt_zip"
    ]

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
    def capitalize_values(df):
        df = df.map(lambda x: x.title() if isinstance(x, str) else x)
        return df

    @staticmethod
    def cross_match(input_df, reference_df, matching_cols):
        """Cross match the columns of each row of each df and return the list of matched columns from the reference dataframe."""
        
        try:
            # Validate the needed columns for matching

            if (input_df is not None and reference_df is not None and 
            CSVProcessor.validate_csv(reference_df, CSVProcessor.valid_ref_data_cols)):
            
                non_phone_cols = [col for col in reference_df.columns if col not in ["phone_1", "phone_2", "phone_3", "phone_1_type", "phone_2_type", "phone_3_type"]]

                melted_reference_df = pd.melt(reference_df, 
                                                id_vars=non_phone_cols, 
                                                value_vars=["phone_1", "phone_2", "phone_3"], 
                                                var_name="phone_type", 
                                                value_name="phone")

                reference_df = melted_reference_df.drop(columns=['phone_type']).drop_duplicates()

                if "phone" in matching_cols:
                    reference_df = reference_df.dropna(subset=['phone'])
                print("indf: ", input_df.columns)
                print("input df: ", input_df[matching_cols])
                input_df = input_df[matching_cols]
                
                
                all_matches = pd.merge(reference_df, input_df, on=matching_cols, how="inner").drop_duplicates()

                # Drop unnecessary or redundant columns
                all_matches = all_matches.loc[:, ~all_matches.columns.duplicated()]

                # Resolve duplicates by keeping the row with the most non-null columns
                all_matches['non_null_count'] = all_matches.notnull().sum(axis=1)
                all_matches = all_matches.sort_values('non_null_count', ascending=False).drop_duplicates(subset=['first_name', 'last_name'], keep='first').drop(columns='non_null_count')
                
                # Capitalize values in specified columns
                all_matches = CSVProcessor.capitalize_values(all_matches)

                return all_matches
            
        except Exception as e:
            print(f"An error occurred: {e}")

        return pd.DataFrame()


input_file = r"backend\agentdata\reverse_prospect_data\vicky_reverse.csv"
reference_file = r"backend\agentdata\aggregate_data\master_data.csv"
input_df = CSVUtilities.load_csv(input_file)
ref_df = CSVUtilities.load_csv(reference_file)
# valid_data_cols = ["first_name", "last_name", "email", "office_name", "phone"]

matched_df = CSVProcessor.cross_match(input_df, ref_df, ["first_name", "last_name"])
matched_df.to_csv(r"backend\agentdata\matched_data\matched_data1.csv", index=False)


