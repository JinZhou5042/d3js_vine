from flask import Flask, render_template, jsonify, Response, abort, send_from_directory
from generate_d3_input import generate_log_data
import os


app = Flask(__name__)

LOGS_DIR = 'logs'

@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    log_folders_sorted = sorted(log_folders)
    return render_template('index.html', log_folders=log_folders_sorted)

@app.route('/report')
def report():
    return render_template('report.html')

@app.route('/debug')
def debug():
    return render_template('debug.html')

@app.route('/performance')
def performance():
    return render_template('performance.html')

@app.route('/transactions')
def transactions():
    return render_template('transactions.html')

@app.route('/input-path/<log_folder>')
def input_path(log_folder):
    input_folder_path = os.path.join(LOGS_DIR, log_folder, 'vine-logs')
    print(input_folder_path)
    if os.path.exists(input_folder_path) and os.path.isdir(input_folder_path):
        return jsonify({'inputPath': input_folder_path})
    return jsonify({'error': 'Input folder not found'}), 404

@app.route('/data/<path:filename>')
def serve_from_data(filename):
    return send_from_directory('data', filename)

@app.route('/logs/<path:filename>')
def serve_file(filename):
    base_directory = os.path.abspath("logs/")
    file_path = os.path.join(base_directory, filename)

    # stream the file
    def generate():
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(4096)  # 读取固定大小的数据块
                if not chunk:
                    break
                yield chunk

    return Response(generate(), mimetype='text/plain')

def process_single_log(log_dir, data_dir):
    print(f"Processing Log: {log_dir} ...")

    if os.path.isdir(log_dir) and os.listdir(log_dir):
        if not os.path.exists(data_dir) or not os.path.isdir(data_dir):
            os.makedirs(data_dir, exist_ok=True)
        
        generate_log_data(log_dir, data_dir)


def process_logs():
    print("Processing Logs ...")
    for log in os.listdir(LOGS_DIR):
        log_dir = os.path.join(LOGS_DIR, log)
        if os.path.isdir(log_dir):
            # if the folder does not contain vine-logs, or it is empty, skip
            if not os.path.exists(os.path.join(log_dir, 'vine-logs')) or not os.listdir(os.path.join(log_dir, 'vine-logs')):
                continue 
            data_dir = os.path.join(log_dir, 'vine-logs')
            process_single_log(log_dir, data_dir)


if __name__ == '__main__':
    process_logs()
    app.run(host='0.0.0.0', port=9122, debug=True)
