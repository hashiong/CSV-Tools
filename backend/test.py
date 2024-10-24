
from utils.processing import load_and_preprocess_data, melt_master_dataframe, find_missing_rows, count_matching_names
import pandas as pd

input_df = pd.read_csv(r'C:\ReMax\unique.csv')

input_df2 = pd.read_csv(r'C:\ReMax\inputunique.csv')


print(find_missing_rows(input_df, input_df2, ['First Name', 'Last Name']))