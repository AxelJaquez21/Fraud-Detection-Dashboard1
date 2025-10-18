from flask import Flask, render_template, request, redirect, url_for, jsonify
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
# Upload Page
# ==============================
@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        file = request.files['file']
        if file:
            file_bytes = file.read()
            filename = file.filename

            # Start background thread for processing
            threading.Thread(target=process_data, args=(file_bytes, filename)).start()

            # Redirect to loading screen
            return redirect(url_for('loading_page'))

    return render_template("upload.html")


# ==============================
# Background Data Processing
# ==============================
def process_data(file_bytes, filename):
    global processed_data
    time.sleep(3)  # simulate some loading time

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

    # Normalize column names (lowercase, no spaces)
    df.columns = [col.strip().replace(" ", "").lower() for col in df.columns]

    # Add a fraud flag: mark withdrawals > 10,000 as suspicious
    if 'withdrawalamt' in df.columns:
        df['fraud_flag'] = df['withdrawalamt'] > 10000
    else:
        df['fraud_flag'] = False

    # Map normalized columns back to display names
    normalized_to_display = {
        col.strip().replace(" ", "").lower(): col for col in COLUMNS_DISPLAY
    }

    # Build final processed data for dashboard
    processed_data = []
    for _, row in df.iterrows():
        row_dict = {}
        for norm_col, display_col in normalized_to_display.items():
            if norm_col in df.columns:
                row_dict[display_col] = row.get(norm_col, "")
        row_dict['fraud_flag'] = bool(row.get('fraud_flag', False))
        processed_data.append(row_dict)


# ==============================
# Loading Page
# ==============================
@app.route("/loading")
def loading_page():
    return render_template("loading.html")


# ==============================
# Ready Check Endpoint
# ==============================
@app.route("/is_ready")
def is_ready():
    """AJAX endpoint for loading page to check if processing is done"""
    return jsonify({'ready': len(processed_data) > 0})


# ==============================
# Dashboard Page
# ==============================
@app.route("/dashboard")
def dashboard():
    """Display processed transaction data"""
    return render_template("dashboard.html", transactions=processed_data)


# ==============================
# Run the App
# ==============================
if __name__ == "__main__":
    app.run(debug=True)
