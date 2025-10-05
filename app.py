from flask import Flask, request, render_template, jsonify
import pandas as pd
import time  # For cache busting in CSS

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html', time=time.time)

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']

    try:
        # Detect file type and read accordingly
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file)
        else:
            return jsonify({'error': 'Unsupported file type'}), 400

        # Example processing: add a simple fraud flag
        if 'amount' in df.columns:
            df['fraud_flag'] = df['amount'] > 10000
        else:
            df['fraud_flag'] = False

        # Convert to JSON to send back to front-end
        result = df.to_dict(orient='records')
        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
