from flask import Flask, render_template, jsonify, send_from_directory
from generate_d3_input import generate_log_data
import os

app = Flask(__name__)

LOGS_DIR = 'logs'  # logs 文件夹的路径

@app.route('/')
def index():
    log_folders = [name for name in os.listdir(LOGS_DIR) if os.path.isdir(os.path.join(LOGS_DIR, name))]
    return render_template('index.html', log_folders=log_folders)

@app.route('/input-path/<log_folder>')
def input_path(log_folder):
    input_folder_path = os.path.join(LOGS_DIR, log_folder, 'vine-logs')
    print(input_folder_path)
    if os.path.exists(input_folder_path) and os.path.isdir(input_folder_path):
        return jsonify({'inputPath': input_folder_path})
    return jsonify({'error': 'Input folder not found'}), 404

@app.route('/logs/<path:filename>')
def custom_static(filename):
    return send_from_directory('logs', filename)

import os

def process_logs():
    if not os.path.isdir(LOGS_DIR):
        print(f"Provided path '{LOGS_DIR}' is not a directory.")
        return

    for log in os.listdir(LOGS_DIR):
        print(f"Processing Log: {log} ...")
        log_dir = os.path.join(LOGS_DIR, log)
        if os.path.isdir(log_dir):
            data_dir = os.path.join(log_dir, 'vine-logs')
            generate_log_data(log_dir, data_dir)

if __name__ == '__main__':
    process_logs()
    app.run(debug=True)
