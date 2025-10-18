from flask import Flask, render_template, request, jsonify
import pandas as pd
import threading
import io
import time

app = Flask(__name__)

# Global storage for processed data
processed_data = []

# Columns to display on dashboard
COLUMNS_DISPLAY = [
    'Account No', 'DATE', 'TRANSACTION DETAILS', 'VALUE DATE',
    'WITHDRAWAL AMT', 'DEPOSIT AMT', 'BALANCE AMT'
]

# ==============================
# Upload & Processing
# ==============================
@app.route("/", methods=["GET", "POST"])
def upload_file():
    global processed_data
    if request.method == "POST":
        file = request.files.get('file')
        if file:
            file_bytes = file.read()
            filename = file.filename

            # Reset previous data
            processed_data = []

            # Start background processing thread
            threading.Thread(target=process_data, args=(file_bytes, filename)).start()

            # Return immediately (frontend will poll for data)
            return '', 202  # HTTP 202 Accepted

    return render_template("upload.html")  # single-page upload + dashboard

# ==============================
# Background Data Processing
# ==============================
def process_data(file_bytes, filename):
    global processed_data
    time.sleep(2)  # simulate processing time

    buffer = io.BytesIO(file_bytes)

    # Read CSV or Excel
    if filename.endswith('.csv'):
        df = pd.read_csv(buffer)
    elif filename.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(buffer)
    else:
        df = pd.DataFrame({'error': ['Unsupported file type']})
        processed_data = df.to_dict(orient='records')
        return

    # Normalize columns (lowercase, no spaces)
    df.columns = [col.strip().replace(" ", "").lower() for col in df.columns]

    # Add fraud flag (withdrawals > 10,000)
    if 'withdrawalamt' in df.columns:
        df['fraud_flag'] = df['withdrawalamt'] > 10000
    else:
        df['fraud_flag'] = False

    # Map normalized columns to display names
    normalized_to_display = {col.strip().replace(" ", "").lower(): col for col in COLUMNS_DISPLAY}

    # Build processed data list
    processed_data = []
    for _, row in df.iterrows():
        row_dict = {}
        for norm_col, display_col in normalized_to_display.items():
            if norm_col in df.columns:
                row_dict[display_col] = row.get(norm_col, "")
        row_dict['fraud_flag'] = bool(row.get('fraud_flag', False))
        processed_data.append(row_dict)

# ==============================
# Endpoint: Check if data is ready
# ==============================
@app.route("/is_ready")
def is_ready():
    return jsonify({'ready': len(processed_data) > 0})

# ==============================
# Endpoint: Fetch processed data
# ==============================
@app.route("/get_data")
def get_data():
    return jsonify(processed_data)

# ==============================
# Run the app
# ==============================
if __name__ == "__main__":
    app.run(debug=True)
