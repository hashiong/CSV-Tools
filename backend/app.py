from flask import Flask, request, jsonify, send_file
import pandas as pd
import sys
import os
from io import BytesIO

# Add the project root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.csv_processor import CSVProcessor  # Import CSVProcessor from the backend package

app = Flask(__name__)

@app.route('/api/cross-match', methods=['POST'])
def cross_match_api():
    try:
        # Ensure input file is provided
        if 'input_file' not in request.files:
            return jsonify({"error": "No input file provided."}), 400
        
        input_file = request.files['input_file']

        # Ensure matching columns are provided in the form data
        matching_cols = request.form.getlist('matching_cols')
        if not matching_cols:
            return jsonify({"error": "No matching columns specified."}), 400
        
        # Check if "name" is in matching_cols and replace it with ["first_name", "last_name"]
        if "name" in matching_cols:
            matching_cols.remove("name")
            matching_cols.extend(["first_name", "last_name"])

        # Path to the reference file
        reference_file = os.path.join(os.path.dirname(__file__), 'agentdata', 'aggregate_data', 'master_data.csv')

        # Load input file into DataFrame using BytesIO
        input_df = pd.read_csv(BytesIO(input_file.read()))
        reference_df = pd.read_csv(reference_file)
        
        # Validate if necessary columns are present
        if input_df.empty or reference_df.empty:
            return jsonify({"error": "One or both files are empty."}), 400

        # Ensure columns for matching exist in both DataFrames
        missing_cols = [col for col in matching_cols if col not in input_df.columns or col not in reference_df.columns]
        if missing_cols:
            return jsonify({"error": f"Missing columns: {', '.join(missing_cols)}"}), 400

        # Perform cross-match using the CSVProcessor class, passing the columns for matching
        matched_df = CSVProcessor.cross_match(input_df, reference_df, matching_cols)

        # Check if any matches were found
        if matched_df.empty:
            return jsonify({"message": "No matches found."}), 200

        # Generate the CSV content in memory using BytesIO
        csv_output = BytesIO()
        matched_df.to_csv(csv_output, index=False)
        csv_output.seek(0)  # Go to the start of the BytesIO object

        # Return the CSV file as a response for download
        return send_file(
            csv_output,
            mimetype="text/csv",
            as_attachment=True,
            download_name="matched_data.csv"  # For Flask versions >= 2.0
        )

    except Exception as e:
        # Catch any unexpected errors and return an appropriate response
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
