import sys
import argparse
import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import shutil
from datetime import datetime
import re
import ast
import numpy as np


def parse_txn(txn):
    log_lines = open(txn, 'r').read().splitlines()

    task_info = []
    library_info = []
    worker_info = {}
    manager_info = {}
    worker_slots = {}

    for line in log_lines:
        if line.startswith("#"):
            continue
        
        timestamp, _, category, obj_id, status, *info = line.split(maxsplit=5)
        try:
            timestamp = float(timestamp) / 1000000
        except ValueError:
            continue

        info = info[0] if info else "{}"
        if category == 'TASK':
            if status == 'READY':
                category = info.split()[0]
                resources_requested = json.loads(info.split(' ', 3)[-1])
                task = {
                    'task_id': obj_id,

                    # Timestamps throughout the task lifecycle
                    'when_ready': timestamp,          # ready status on the manager
                    'time_commit_start': None,              # start commiting to worker
                    'time_commit_end': None,                # end commiting to worker
                    'when_running': None,             # running status on worker
                    'time_worker_start': None,              # start executing on worker
                    'time_worker_end': None,                # end executing on worker
                    'when_waiting_retrieval': None,   # waiting for retrieval status on worker
                    'when_retrieved': None,           # retrieved status on worker
                    'when_done': None,                # done status on worker

                    'worker_committed': None,
                    'worker_id': None,
                    'worker_slot': None,

                    'size_input_mgr': None,
                    'size_output_mgr': None,
                    'cores_requested': resources_requested.get("cores", [0, ""])[0],
                    'gpus_requested': resources_requested.get("gpus", [0, ""])[0],
                    'memory_requested(MB)': resources_requested.get("memory", [0, ""])[0],
                    'disk_requested(MB)': resources_requested.get("disk", [0, ""])[0],
                    'retrieved_status': None,
                    'done_status': None,
                    'done_code': None,
                    'category': category,

                    #############################
                    # Not yet implemented
                    # 'when_input_sent': None,
                    # 'when_output_received': None,
                    #############################
                }
                task_info.append(task)
            if status == 'RUNNING':
                # a running task can be a library which does not have a ready status
                is_library = True
                resources_allocated = json.loads(info.split(' ', 3)[-1])
                for task in task_info:
                    if task['task_id'] == obj_id:
                        is_library = False
                        task['when_running'] = timestamp
                        task['worker_committed'] = info.split()[0]
                        task['time_commit_start'] = resources_allocated["time_commit_start"][0]
                        task['time_commit_end'] = resources_allocated["time_commit_end"][0]
                        task['size_input_mgr'] = resources_allocated["size_input_mgr"][0]

                        # assign a slot to task
                        worker_hash = task['worker_committed']
                        if worker_hash not in worker_slots:
                            worker_slots[worker_hash] = []
                        slots = worker_slots[worker_hash]
                        slot_found = False
                        for slot_id, slot in enumerate(slots):
                            if not slot['in_use']:
                                slot['tasks'].append(task)
                                task['worker_slot'] = slot_id + 1
                                slot['in_use'] = True
                                slot_found = True
                                break
                        if not slot_found:
                            new_slot = {'tasks': [task], 'in_use': True}
                            slots.append(new_slot)
                            task['worker_slot'] = len(slots)
                        break
                if is_library:
                    library = {
                        'task_id': obj_id,
                        'when_running': timestamp,
                        'time_commit_start': resources_allocated["time_commit_start"][0],
                        'time_commit_end': resources_allocated["time_commit_end"][0],
                        'when_sent': None,
                        'when_started': None,
                        'when_retrieved': None,
                        'worker_committed': info.split(' ', 3)[0],
                        'worker_id': None,
                        'size_input_mgr': resources_allocated["size_input_mgr"][0],
                        'cores_requested': resources_allocated.get("cores", [0, ""])[0],
                        'gpus_requested': resources_allocated.get("gpus", [0, ""])[0],
                        'memory_requested(MB)': resources_allocated.get("memory", [0, ""])[0],
                        'disk_requested(MB)': resources_allocated.get("disk", [0, ""])[0],
                    }
                    library_info.append(library)
            if status == 'WAITING_RETRIEVAL':
                for task in task_info:
                    if task['task_id'] == obj_id:
                        task['when_waiting_retrieval'] = timestamp
                        slots = worker_slots[task['worker_committed']]
                        for slot in slots:
                            if task in slot['tasks']:
                                slot['in_use'] = False
                                break
                        break
            if status == 'RETRIEVED':
                try:
                    resources_retrieved = json.loads(info.split(' ', 5)[-1])
                except json.JSONDecodeError:
                    resources_retrieved = {}

                is_library = True
                for task in task_info:
                    if task['task_id'] == obj_id:
                        is_library = False
                        task['when_retrieved'] = timestamp
                        task['retrieved_status'] = status
                        task['time_worker_start'] = resources_retrieved.get("time_worker_start", [None])[0]
                        task['time_worker_end'] = resources_retrieved.get("time_worker_end", [None])[0]
                        task['size_output_mgr'] = resources_retrieved.get("size_output_mgr", [None])[0]
                        break
                if is_library:
                    for library in library_info:
                        if library['task_id'] == obj_id:
                            library['when_retrieved'] = timestamp
                            break
            if status == 'DONE':
                done_info = info.split() if info else []
                for task in task_info:
                    if task['task_id'] == obj_id:
                        task['when_done'] = timestamp
                        task['done_status'] = done_info[0] if len(done_info) > 0 else None
                        task['done_code'] = done_info[1] if len(done_info) > 1 else None
                        break
        if category == 'WORKER':
            if not obj_id.startswith('worker'):
                continue
            if status == 'CONNECTION':
                worker_info[obj_id] = {
                    'time_connected': timestamp,
                    'time_disconnected': None,
                    'worker_id': None,
                    'resources_reported': {},
                    'input_transfer': {},
                    'output_transfer': {},
                    'cache_update': {},
                }
            if status == 'DISCONNECTION':
                worker_info[obj_id]['time_disconnected'] = timestamp
            if status == 'RESOURCES':
                resources = json.loads(info)
                resources_reported_id = len(worker_info[obj_id]['resources_reported'])
                worker_info[obj_id]['resources_reported'][resources_reported_id] = {
                    'time': timestamp,
                    'cores': resources.get("cores", [0, ""])[0],
                    'memory(MB)': resources.get("memory", [0, ""])[0],
                    'disk(MB)': resources.get("disk", [0, ""])[0],
                }
            if status == 'TRANSFER':
                transfer_type, filename, transfer_size, wall_time, start_time = info.split(' ', 4)
                if transfer_type == 'INPUT':
                    input_transfer_id = len(worker_info[obj_id]['input_transfer'])
                    worker_info[obj_id]['input_transfer'][input_transfer_id] = {
                        'filename': filename,
                        'transfer_size(MB)': transfer_size,
                        'start_time': start_time,
                        'wall_time': wall_time,
                    }
                if transfer_type == 'OUTPUT':
                    output_transfer_id = len(worker_info[obj_id]['output_transfer'])
                    worker_info[obj_id]['output_transfer'][output_transfer_id] = {
                        'filename': filename,
                        'transfer_size(MB)': transfer_size,
                        'start_time': start_time,
                        'wall_time': wall_time,
                    }
            if status == 'CACHE_UPDATE':
                filename, size_in_mb, wall_time, start_time = info.split(' ', 3)
                cache_update_id = len(worker_info[obj_id]['cache_update'])
                worker_info[obj_id]['cache_update'][cache_update_id] = {
                    'filename': filename,
                    'size(MB)': size_in_mb,
                    'start_time': start_time,
                    'wall_time': wall_time,
                }

        if category == 'LIBRARY':
            if status == 'SENT':
                for library in library_info:
                    if library['task_id'] == obj_id:
                        library['when_sent'] = timestamp
            if status == 'STARTED':
                for library in library_info:
                    if library['task_id'] == obj_id:
                        library['when_started'] = timestamp
        if category == 'MANAGER':
            if status == 'START':
                manager_info['time_start'] = timestamp
            if status == 'END':
                manager_info['time_end'] = timestamp

    dirname = os.path.dirname(txn)

    # add a worker_id for each data structure
    worker_info = {k: v for k, v in sorted(worker_info.items(), key=lambda item: item[1]['time_connected'])}
    for i, worker in enumerate(worker_info, start=1):
        worker_info[worker]['worker_id'] = i
    for task in task_info:
        worker_hash = task['worker_committed']
        task['worker_id'] = worker_info[worker_hash]['worker_id']
    for library in library_info:
        worker_hash = library['worker_committed']
        library['worker_id'] = worker_info[worker_hash]['worker_id']

    # Convert lists to DataFrames
    task_df = pd.DataFrame(task_info)
    library_df = pd.DataFrame(library_info)

    # Save DataFrames to CSV
    task_df.to_csv(os.path.join(dirname, 'task_info.csv'), index=False)
    library_df.to_csv(os.path.join(dirname, 'library_info.csv'), index=False)

    with open(os.path.join(dirname, 'worker_info.json'), 'w') as f:
        json.dump(worker_info, f, indent=4)


def transform_data(df):
    # calculate the log of run time
    df['Log Run Time'] = np.log1p(df['Run Time'])
    # combine worker and slot to form a unique identifier
    df['Worker-Slot'] = df['Worker'] + '-' + df['Slot']
    return df


def parse_debug(data_dir):
    debug = os.path.join(data_dir, 'debug')
    
    worker_configs = {}
    collecting_resources_for_worker = None
    with open(debug, 'r') as file:
        for line in file:
            # 2024/03/12 20:37:39.26 vine_manager[2054845] tcp: accepted connection from 10.32.79.53 port 53616
            accepted_conn_match = re.search(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{2}) vine_manager\[\d+\] tcp: accepted connection from (\d+\.\d+\.\d+\.\d+) port (\d+)', line)
            if accepted_conn_match:
                connected_time_str = accepted_conn_match.group(1)
                ip = accepted_conn_match.group(2)
                port = accepted_conn_match.group(3)
                connected_time = datetime.strptime(connected_time_str, '%Y/%m/%d %H:%M:%S.%f')
                key = f"{ip}:{port}"
                if key not in worker_configs:
                    worker_configs[key] = {'id': None, 'ip': ip, 'port': port, 'hostname': None, 'worker_hash': None, 'function_slots': None,
                                           'cores': None, 'memory': None, 'disk': None, 'gpus': None, 'tag': None, 
                                           'CCTools_Version': None, 'architecture': None, 'provides_library': [], 
                                           'connected_time': connected_time, 'end_time': None, 'lifetime': None,
                                           'tasks_done': None, 'avg_task_runtime': None, 'max_task_runtime': None, 'min_task_runtime': None,}
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: rx from unknown (10.32.79.53:53616): taskvine 8 d8civy095.crc.nd.edu Linux x86_64 8.0.0
            hostname_match = re.search(r'rx from unknown \((\d+\.\d+\.\d+\.\d+):(\d+)\): .+ (\S+\.nd\.edu)', line)
            if hostname_match:
                ip = hostname_match.group(1)
                port = hostname_match.group(2)
                hostname = hostname_match.group(3)
                key = f"{ip}:{port}"
                if key in worker_configs:
                    worker_configs[key]['hostname'] = hostname
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: rx from d8civy026.crc.nd.edu (10.32.78.171:55042): info worker-id worker-ff9c317971b9f77f81dba73e8f929cd1
            worker_hash_match = re.search(r'info worker-id (\S+)', line)
            if worker_hash_match:
                worker_hash = worker_hash_match.group(1)
                ip_port_match = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line)
                if ip_port_match:
                    ip = ip_port_match.group(1)
                    port = ip_port_match.group(2)
                    key = f"{ip}:{port}"
                    if key in worker_configs:
                        worker_configs[key]['worker_hash'] = worker_hash
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: rx from d8civy095.crc.nd.edu (10.32.79.53:53616): resources
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: cores 4
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: memory 8192
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: disk 4644
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: gpus 0
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: workers 1
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: tag 0
            # 2024/03/12 20:37:39.26 vine_manager[2054845] vine: end
            if 'resources' in line:
                ip_port_match = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line)
                if ip_port_match:
                    ip = ip_port_match.group(1)
                    port = ip_port_match.group(2)
                    collecting_resources_for_worker = f"{ip}:{port}"
                continue
            if collecting_resources_for_worker:
                match = re.search(r'.*?vine: ([a-zA-Z]+) (\d+)', line)
                if match:
                    resource_type, value = match.groups()
                    if resource_type in worker_configs[key]:
                        worker_configs[collecting_resources_for_worker][resource_type] = value
                if 'end' in line:
                    collecting_resources_for_worker = None
            # 2024/03/12 20:37:39.59 vine_manager[2054845] vine: d8civy121.crc.nd.edu (10.32.79.105:48904) running CCTools version 8.0.0 on Linux (operating system) with architecture x86_64 is ready
            cctools_arch_match = re.search(r'running CCTools version (\S+) on .+ with architecture (\S+)', line)
            if cctools_arch_match:
                cctools_version, architecture = cctools_arch_match.groups()
                ip_port_match = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line)
                if ip_port_match:
                    ip = ip_port_match.group(1)
                    port = ip_port_match.group(2)
                    key = f"{ip}:{port}"
                    if key in worker_configs:
                        worker_configs[key]['CCTools_Version'] = cctools_version
                        worker_configs[key]['architecture'] = architecture
            # 2024/03/12 20:37:39.70 vine_manager[2054845] vine: tx to d12chas323.crc.nd.edu (10.32.84.153:53836): provides_library test-library
            provides_library_match = re.search(r'provides_library (\S+)', line)
            if provides_library_match:
                library = provides_library_match.group(1)
                ip_port_match = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line)
                if ip_port_match:
                    ip = ip_port_match.group(1)
                    port = ip_port_match.group(2)
                    key = f"{ip}:{port}"
                    if key in worker_configs and library not in worker_configs[key]['provides_library']:
                        worker_configs[key]['provides_library'].append(library)
            # 2024/03/12 20:37:39.70 vine_manager[2054845] vine: tx to d12chas323.crc.nd.edu (10.32.84.153:53836): function_slots 4
            function_slots_match = re.search(r'function_slots (\d+)', line)
            if function_slots_match:
                function_slots = function_slots_match.group(1)
                ip_port_match = re.search(r'(\d+\.\d+\.\d+\.\d+):(\d+)', line)
                if ip_port_match:
                    ip = ip_port_match.group(1)
                    port = ip_port_match.group(2)
                    key = f"{ip}:{port}"
                    if key in worker_configs:
                        worker_configs[key]['function_slots'] = function_slots
            # 2024/03/18 10:00:00.63 vine_manager[1541931] vine: worker d32cepyc045.crc.nd.edu (10.32.89.209:44926) removed
            match = re.search(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{2}) .*? vine: worker .+? \((\d+\.\d+\.\d+\.\d+):(\d+)\) removed', line)
            if match:
                end_time_str = match.group(1)
                end_time = datetime.strptime(end_time_str, '%Y/%m/%d %H:%M:%S.%f')
                ip, port = match.group(2), match.group(3)
                key = f"{ip}:{port}"
                if key in worker_configs:
                    worker_configs[key]['end_time'] = end_time
                    worker_configs[key]['lifetime'] = (worker_configs[key]['end_time'] - worker_configs[key]['connected_time']).total_seconds()
                else:
                    print("Key not found in worker configs.")
            # 2024/03/12 19:36:02.59 vine_manager[1960177] vine: d12chas328.crc.nd.edu (10.32.84.163:49710) done in 6.86s total tasks 2 average 4.90s
            match = re.search(r'(\d+\.\d+\.\d+\.\d+:\d+).+done in (\S+)s total tasks (\d+) average (\S+)s', line)
            if match:
                ip_port = match.group(1)  # IP和端口
                task_runtime = float(match.group(2))
                total_tasks_done = int(match.group(3))
                avg_task_runtime = float(match.group(4))
                key = ip_port
                if key in worker_configs:
                    worker_configs[key]['tasks_done'] = total_tasks_done
                    worker_configs[key]['avg_task_runtime'] = avg_task_runtime
                    if not worker_configs[key]['max_task_runtime']:
                        worker_configs[key]['max_task_runtime'] = task_runtime
                    else:
                        worker_configs[key]['max_task_runtime'] = max(worker_configs[key]['max_task_runtime'], task_runtime)
                    if not worker_configs[key]['min_task_runtime']:
                        worker_configs[key]['min_task_runtime'] = task_runtime
                    else:
                        worker_configs[key]['min_task_runtime'] = min(worker_configs[key]['min_task_runtime'], task_runtime)

    worker_list = [value for key, value in worker_configs.items()]
    sorted_worker_list = sorted(worker_list, key=lambda x: (x['worker_hash'] is None, x['worker_hash']))
    for i, worker in enumerate(sorted_worker_list, start=1):
        worker['id'] = i
    sorted_worker_configs = {f"{worker['ip']}:{worker['port']}": worker for worker in sorted_worker_list}

    worker_configs_df = pd.DataFrame.from_dict(sorted_worker_configs, orient='index')
    all_workers_configs = pd.read_csv('all_worker_configs.csv')
    worker_configs_df = pd.merge(worker_configs_df, all_workers_configs, on='hostname', how='left')

    worker_configs_df.to_csv(os.path.join(data_dir, 'workerConfigs.csv'), index=False)


def generate_log_data(log_dir):
    txn = os.path.join(log_dir, 'vine-logs', 'transactions')
    parse_txn(txn)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="This script is used to plot a figure for a transaction log")
    parser.add_argument('--log-dir', type=str, default='most-recent')
    parser.add_argument('--print', action='store_true')
    args = parser.parse_args()

    log_dir = args.log_dir
    data_dir = os.path.join(log_dir, 'vine-logs')

    generate_log_data(log_dir)