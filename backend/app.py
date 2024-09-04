from flask import Flask, request, jsonify
import pandas as pd
import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend import CSVProcessor  # Import CSVProcessor from the backend package

app = Flask(__name__)

# Your existing cross-match functions and other utility functions go here

@app.route('/api/cross-match', methods=['POST'])
def cross_match_api():
    if request.method == 'POST':
        input_file = request.files['input_file']
        reference_file = r"backend/agentdata/aggregate_data/cleaned_master_data.csv"
        
        # Load the uploaded files into dataframes
        input_df = pd.read_csv(input_file)
        reference_df = pd.read_csv(reference_file)
        
        valid_data_cols = ["first_name", "last_name", "email", "office_name", "phone"]
        
        # Perform cross-match
        matched_df = CSVProcessor.cross_match(input_df, reference_df)

        # Check if matched_df is None or empty
        if matched_df is None or matched_df.empty:
            return jsonify([])  # Return an empty JSON array

        # Return matched data as JSON
        return jsonify(matched_df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
