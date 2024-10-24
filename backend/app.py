import os
from flask import Flask, render_template, request, send_file
from utils.processing import load_and_preprocess_data, melt_master_dataframe, find_missing_rows, count_matching_names
from utils.input_utils.input_processor import check_columns, clean_phone_numbers
import pandas as pd

app = Flask(__name__, template_folder="../frontend/templates", static_folder="../frontend/static")

UPLOAD_FOLDER = 'data/upload'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_FILE_PATH = os.path.join(BASE_DIR, 'data', 'brokermetrics_data', 'Master', '10232024.csv')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        input_file = request.files["input_file"]
        
        # Save uploaded file
        input_file_path = os.path.join(UPLOAD_FOLDER, input_file.filename)
        input_file.save(input_file_path)

        # Load and preprocess the uploaded input and master CSV files
        input_df, master_df = load_and_preprocess_data(input_file_path, MASTER_FILE_PATH)
        if(check_columns(input_df, ["First Name", "Last Name", "Phone"])):
            clean_phone_numbers(input_df, ["Phone"])
        else:
            print("cols dont match")

        # Melt the master DataFrame to handle multiple phone numbers
        melted_master_df = melt_master_dataframe(master_df)

        # Find missing agents
        missing_agents = find_missing_rows(melted_master_df, input_df, ['First Name', 'Last Name'])
        missing_agents = missing_agents[missing_agents['Phone'].isnull()]
        missing_agents.to_csv("missing.csv")

        # Merge the data based on 'First Name' and 'Last Name'
        merged_df = pd.merge(input_df, melted_master_df, on=['First Name', 'Last Name'], how='left', suffixes=('', '_data'))
        merged_df['Phone'] = merged_df['Phone'].fillna(merged_df['Phone_data'])
        merged_df.drop(columns=['Phone_data'], inplace=True)
        merged_df = merged_df[merged_df['Phone'].notna()]
        cleaned_df = merged_df.drop_duplicates(subset=['First Name', 'Last Name', "Phone"])

        unique_agents = merged_df.drop_duplicates(subset=['First Name', 'Last Name'])
        unique_agents.to_csv("unique_agents.csv", index=False)

        print(f'Total rows in input: {len(input_df)}')
        print(f'Match Rate: {len(unique_agents) / len(input_df) * 100:.2f}%')
        print(f'Total agents matched and found phone numbers: {len(unique_agents)}')
        print(f'Total Phone Numbers Found: {len(cleaned_df)}')

        # Save the merged output to a CSV file
        output_file_path = os.path.join(UPLOAD_FOLDER, "matched_result.csv")
        cleaned_df.to_csv(output_file_path, index=False)

        # Return the file as a download
        return send_file(output_file_path, as_attachment=True)

    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
