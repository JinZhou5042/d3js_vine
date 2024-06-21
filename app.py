from flask import Flask, render_template, jsonify, Response, abort, send_from_directory
from generate_d3_input import generate_data
import os
import argparse
import sys
import subprocess

def kill_process_on_port(port):
    try:
        # Find the process running on the specified port
        result = subprocess.check_output(["lsof", "-t", "-i:{}".format(port)])
        pid = result.decode().strip()
        if pid:
            print("Process found, PID:", pid)
            # Kill the process
            subprocess.check_output(["kill", pid])
            print("Process has been killed")
        else:
            print("No process found on port {}.".format(port))
    except subprocess.CalledProcessError:
        print("No process found on port {}.".format(port))
    except Exception as e:
        print("An error occurred:", e)
        sys.exit(1)

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

@app.route('/logs/<log_folder>')
def logs(log_folder):
    log_folder_path = os.path.join(LOGS_DIR, log_folder, 'vine-logs')
    if os.path.exists(log_folder_path) and os.path.isdir(log_folder_path):
        return jsonify({'logPath': log_folder_path})
    return jsonify({'error': 'Log folder not found'}), 404

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
                chunk = f.read(4096)
                if not chunk:
                    break
                yield chunk

    return Response(generate(), mimetype='text/plain')


def process_logs():
    for log in os.listdir(LOGS_DIR):
        log_dir = os.path.join(LOGS_DIR, log)
        if os.path.isdir(log_dir):
            # if the folder does not contain vine-logs, or it is empty, skip
            if not os.path.exists(os.path.join(log_dir, 'vine-logs')) or not os.listdir(os.path.join(log_dir, 'vine-logs')):
                continue 

            print(f"Processing Log: {log_dir} ...")
            generate_data(log_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--generate-data', default=False)
    args = parser.parse_args()

    if args.generate_data:
        process_logs()
        
    kill_process_on_port(9122)
    app.run(host='0.0.0.0', port=9122, debug=True)
