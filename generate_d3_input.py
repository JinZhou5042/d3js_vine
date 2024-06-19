import sys
import argparse
import os
import json
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
from bitarray import bitarray
import ast
import numpy as np


def parse_txn(txn):
    task_info = {}
    library_info = {}
    worker_info = {}
    manager_info = {}
    worker_coremap = {}
    task_try_count = {}         # task_id -> try_count
    file_info = {}

    total_lines = 0
    with open(txn, 'r') as file:
        for line in file:
            total_lines += 1

    with open(txn, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing transactions")
        for line in file:
            pbar.update(1)

            if line.startswith("#"):
                continue

            timestamp, _, category, obj_id, status, *info = line.split(maxsplit=5)

            try:
                timestamp = float(timestamp) / 1000000
            except ValueError:
                continue

            info = info[0] if info else "{}"
            if category == 'TASK':
                task_id = int(obj_id)
                if status == 'READY':
                    if task_id not in task_try_count:
                        task_try_count[task_id] = 1
                    else:
                        task = task_info[(task_id, task_try_count[task_id])]
                        task['when_next_ready'] = timestamp
                        # reset the coremap for the new try
                        for i in task['core_id']:
                            worker_coremap[task['worker_committed']][i] = 0
                        task_try_count[task_id] += 1
                    task_category = info.split()[0]
                    try_id = task_try_count[task_id]
                    resources_requested = json.loads(info.split(' ', 3)[-1])
                    task = {
                        'task_id': task_id,
                        'try_id': try_id,
                        'worker_id': -1,
                        'core_id': [],

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
                        'when_next_ready': None,          # only for on-worker failed tasks

                        'worker_committed': None,

                        'size_input_mgr': None,
                        'size_output_mgr': None,
                        'cores_requested': resources_requested.get("cores", [0, ""])[0],
                        'gpus_requested': resources_requested.get("gpus", [0, ""])[0],
                        'memory_requested(MB)': resources_requested.get("memory", [0, ""])[0],
                        'disk_requested(MB)': resources_requested.get("disk", [0, ""])[0],
                        'retrieved_status': None,
                        'done_status': None,
                        'done_code': None,
                        'category': task_category,

                        'input_files': [],
                        'output_files': [],

                    }
                    task_info[(task_id, try_id)] = task
                if status == 'RUNNING':
                    # a running task can be a library which does not have a ready status
                    resources_allocated = json.loads(info.split(' ', 3)[-1])
                    try_id = task_try_count[task_id]
                    if task_id in task_try_count:
                        task = task_info[(task_id, try_id)]
                        worker_hash = info.split()[0]
                        task['when_running'] = timestamp
                        task['worker_committed'] = worker_hash
                        task['time_commit_start'] = float(resources_allocated["time_commit_start"][0])
                        task['time_commit_end'] = float(resources_allocated["time_commit_end"][0])
                        task['size_input_mgr'] = float(resources_allocated["size_input_mgr"][0])
                        coremap = worker_coremap[worker_hash]
                        cores_found = 0
                        for i in range(1, len(coremap)):
                            if coremap[i] == 0:
                                coremap[i] = 1
                                task['core_id'].append(i)
                                cores_found += 1
                                if cores_found == task['cores_requested']:
                                    break
                    else:
                        library = {
                            'task_id': task_id,
                            'when_running': timestamp,
                            'time_commit_start': resources_allocated["time_commit_start"][0],
                            'time_commit_end': resources_allocated["time_commit_end"][0],
                            'when_sent': None,
                            'when_started': None,
                            'when_retrieved': None,
                            'worker_committed': info.split(' ', 3)[0],
                            'worker_id': -1,
                            'size_input_mgr': resources_allocated["size_input_mgr"][0],
                            'cores_requested': resources_allocated.get("cores", [0, ""])[0],
                            'gpus_requested': resources_allocated.get("gpus", [0, ""])[0],
                            'memory_requested(MB)': resources_allocated.get("memory", [0, ""])[0],
                            'disk_requested(MB)': resources_allocated.get("disk", [0, ""])[0],
                        }
                        library_info[task_id] = library
                if status == 'WAITING_RETRIEVAL':
                    if task_id in task_try_count:
                        task = task_info[(task_id, task_try_count[task_id])]
                        task['when_waiting_retrieval'] = timestamp
                        worker_hash = task['worker_committed']
                        for core in task['core_id']:
                            worker_coremap[worker_hash][core] = 0
                if status == 'RETRIEVED':
                    try:
                        resources_retrieved = json.loads(info.split(' ', 5)[-1])
                    except json.JSONDecodeError:
                        resources_retrieved = {}
                    if task_id in task_try_count:
                        task = task_info[(task_id, task_try_count[task_id])]
                        task['when_retrieved'] = timestamp
                        task['retrieved_status'] = status
                        task['time_worker_start'] = resources_retrieved.get("time_worker_start", [None])[0]
                        task['time_worker_end'] = resources_retrieved.get("time_worker_end", [None])[0]
                        task['size_output_mgr'] = resources_retrieved.get("size_output_mgr", [None])[0]
                    else:
                        library = library_info[task_id]
                        library['when_retrieved'] = timestamp
                if status == 'DONE':
                    done_info = info.split() if info else []
                    if task_id in task_try_count:
                        task = task_info[(task_id, task_try_count[task_id])]
                        worker_hash = task['worker_committed']
                        task['when_done'] = timestamp
                        task['done_status'] = done_info[0] if len(done_info) > 0 else None
                        task['done_code'] = done_info[1] if len(done_info) > 1 else None
                        worker_info[worker_hash]['tasks_done'] += 1
            if category == 'WORKER':
                if not obj_id.startswith('worker'):
                    continue
                if status == 'CONNECTION':
                    worker_info[obj_id] = {
                        'time_connected': timestamp,
                        'time_disconnected': None,
                        'worker_id': -1,
                        'tasks_done': 0,
                        'cores': None,
                        'memory(MB)': None,
                        'disk(MB)': None,
                        'disk_update': {},
                        'cached_files': {},
                    }
                if status == 'DISCONNECTION':
                    worker_info[obj_id]['time_disconnected'] = timestamp
                if status == 'RESOURCES':
                    # only parse the first resources reported
                    if worker_info[obj_id]['cores'] is not None:
                        continue
                    resources = json.loads(info)
                    cores, memory, disk = resources.get("cores", [0, ""])[0], resources.get("memory", [0, ""])[0], resources.get("disk", [0, ""])[0]
                    worker_info[obj_id]['cores'] = cores
                    worker_info[obj_id]['memory(MB)'] = memory
                    worker_info[obj_id]['disk(MB)'] = disk
                    # for calculating task core_id
                    worker_coremap[obj_id] = bitarray(cores + 1)
                    worker_coremap[obj_id].setall(0)
                if status == 'TRANSFER' or status == 'CACHE_UPDATE':
                    if status == 'TRANSFER':
                        # don't consider transfer as of now
                        transfer_type, filename, size_in_bytes, wall_time, start_time = info.split(' ', 4)
                    elif status == 'CACHE_UPDATE':
                        transfer_type = 'CACHE_UPDATE'
                        filename, size_in_bytes, wall_time, start_time = info.split(' ', 3)

                    start_time = float(start_time) / 1e6
                    wall_time = float(wall_time) / 1e6
                    # update cached_files table and calculate disk increament
                    size_in_bytes = int(size_in_bytes)
                    if filename in worker_info[obj_id]['cached_files']:
                        disk_increament = size_in_bytes - worker_info[obj_id]['cached_files'][filename]
                        worker_info[obj_id]['cached_files'][filename] = size_in_bytes
                    else:
                        disk_increament = size_in_bytes
                        worker_info[obj_id]['cached_files'][filename] = size_in_bytes

                    disk_update_entry_id = len(worker_info[obj_id]['disk_update'])
                    disk_update_entry = {
                        'filename': filename,
                        'size(MB)': size_in_bytes / 2**20,
                        'start_time': start_time,
                        'wall_time': wall_time,
                        'type': transfer_type,
                        'disk_increament(MB)': disk_increament / 2**20,
                    }
                    worker_info[obj_id]['disk_update'][disk_update_entry_id] = disk_update_entry

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
        pbar.close()
    
    #####################################################
    # Remove invalid workers: workers didnn't commit any task
    active_workers = set()
    for task in task_info.values():
        active_workers.add(task['worker_committed'])
    worker_info = {worker_hash: worker for worker_hash, worker in worker_info.items() if worker_hash in active_workers}
    # Sort workers by time connected
    worker_info = {k: v for k, v in sorted(worker_info.items(), key=lambda item: item[1]['time_connected'])}
    # Add worker_id to worker_info and update that in task_info and library_info
    worker_id = 1
    for worker in worker_info.values():
        worker['worker_id'] = worker_id
        worker_id += 1
    for task in task_info.values():
        if task['worker_committed']:
            task['worker_id'] = worker_info[task['worker_committed']]['worker_id']
    for library in library_info.values():
        if library['worker_committed']:
            library['worker_id'] = worker_info[library['worker_committed']]['worker_id']
    #####################################################

    return task_info, task_try_count, library_info, worker_info, manager_info
    

def parse_taskgraph(taskgraph, task_info, task_try_count):
    total_lines = 0
    with open(taskgraph, 'r') as file:
        for line in file:
            total_lines += 1

    with open(taskgraph, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing taskgraph")
        for line in file:
            pbar.update(1)
            if '->' not in line:
                continue
            left, right = line.split(' -> ')
            left = left.strip().strip('"')
            right = right.strip()[:-1].strip('"')

            # task produces an output file
            if left.startswith('task'):
                filename = right.split('-', 1)[1]
                task_id = int(left.split('-')[1])
                try_id = task_try_count[task_id]
                # task_info[(task_id, try_id)]['output_files'].append(filename)
            # task consumes an input file
            elif right.startswith('task'):
                filename = left.split('-', 1)[1]
                task_id = int(right.split('-')[1])
                try_id = task_try_count[task_id]
                # task_info[(task_id, try_id)]['input_files'].append(filename)
        pbar.close()

    return task_info

def expand_done_task(task):
    cores = task['core_id']
    if len(cores) == 0:
        task['core_id'] = -1
        return task
    elif len(cores) == 1:
        task['core_id'] = cores[0]
        return task
    else:
        task_copies = []
        for core in cores:
            task_copy = task.copy()
            task_copy['core_id'] = core
            task_copies.append(task_copy)
        return task_copies
    
def convert_to_and_save_task_df(task_info, dirname):
    task_df = pd.DataFrame.from_dict(task_info, orient='index')
    task_df.to_csv(os.path.join(dirname, 'task.csv'), index=False)
    
    print("Generating task_done.csv...")
    task_done_df = task_df[task_df['when_done'].notnull()]
    task_done_df = task_done_df.apply(expand_done_task, axis=1)
    task_done_df.to_csv(os.path.join(dirname, 'task_done.csv'), index=False)

    print("Generating task_failed_on_manager.csv...")
    task_failed_on_manager_df = task_df[task_df['when_running'].isnull() & task_df['when_ready'].notnull()]
    task_failed_on_manager_df = task_failed_on_manager_df.apply(expand_done_task, axis=1)
    task_failed_on_manager_df.to_csv(os.path.join(dirname, 'task_failed_on_manager.csv'), index=False)
    
    print("Generating task_failed_on_worker.csv...")
    task_failed_on_worker_df = task_df[task_df['when_running'].notnull() & task_df['when_waiting_retrieval'].isnull()]
    task_failed_on_worker_df = task_failed_on_worker_df.apply(expand_done_task, axis=1)
    task_failed_on_worker_df.to_csv(os.path.join(dirname, 'task_failed_on_worker.csv'), index=False)

    return task_df

def generate_log_data(log_dir):

    dirname = os.path.join(log_dir, 'vine-logs')
    txn = os.path.join(dirname, 'transactions')
    taskgraph = os.path.join(dirname, 'taskgraph')

    task_info, task_try_count, library_info, worker_info, manager_info = parse_txn(txn)
    task_info = parse_taskgraph(taskgraph, task_info, task_try_count)
    
    # Convert lists to DataFrames
    task_df = convert_to_and_save_task_df(task_info, dirname)

    library_df = pd.DataFrame.from_dict(library_info, orient='index')
    library_df.to_csv(os.path.join(dirname, 'library.csv'), index=False)
    with open(os.path.join(dirname, 'manager_info.json'), 'w') as f:
        json.dump(manager_info, f, indent=4)
    with open(os.path.join(dirname, 'worker_info.json'), 'w') as f:
        json.dump(worker_info, f, indent=4)

    # Convert disk_update in worker_info to DataFrame
    print("Generating worker_disk_update.csv...")
    rows = []
    for worker_hash, worker in worker_info.items():
        for disk_update in worker['disk_update'].values():
            row = {
                'worker_hash': worker_hash,
                'worker_id': worker['worker_id'],
            }
            row.update(disk_update)
            rows.append(row)
    disk_update_df = pd.DataFrame(rows)

    # this df may be empty
    if not disk_update_df.empty:
        disk_update_df.sort_values(by=['worker_id', 'start_time'], ascending=[True, True], inplace=True)
        disk_update_df['disk_usage(MB)'] = disk_update_df.groupby('worker_id')['disk_increament(MB)'].cumsum()
        disk_update_df['disk_usage(%)'] = disk_update_df['disk_usage(MB)'] / worker_info[worker_hash]['disk(MB)']
        disk_update_df.to_csv(os.path.join(dirname, 'worker_disk_update.csv'), index=False)

    # convert worker_info to DataFrame
    print("Generating worker_summary.csv...")
    rows = []
    for worker_hash, info in worker_info.items():
        row = {
            'worker_hash': worker_hash,
            'worker_id': info['worker_id'],
            'time_connected': info['time_connected'],
            'time_disconnected': info['time_disconnected'],
            'cores': info['cores'],
            'memory(MB)': info['memory(MB)'],
            'disk(MB)': info['disk(MB)'],
            'tasks_done': 0,
            'peak_disk_usage(MB)': 0,
            'peak_disk_usage(%)': 0,
            'avg_task_runtime(s)': 0,
        }
        # calculate the number of tasks done by this worker
        tasks_committed = task_df[task_df['worker_committed'] == worker_hash]
        tasks_done = tasks_committed[tasks_committed['when_done'] > 0]
        row['tasks_done'] = len(tasks_done)
        # check if this worker has any disk updates
        if not disk_update_df.empty and disk_update_df['worker_hash'].isin([worker_hash]).any():
            row['peak_disk_usage(MB)'] = disk_update_df[disk_update_df['worker_hash'] == worker_hash]['disk_usage(MB)'].max()
            row['peak_disk_usage(%)'] = disk_update_df[disk_update_df['worker_hash'] == worker_hash]['disk_usage(MB)'].max() / info['disk(MB)']
        # the worker may not have any tasks
        if row['tasks_done'] > 0:
            row['avg_task_runtime(s)'] = task_df[task_df['worker_committed'] == worker_hash]['time_worker_end'].mean() - task_df[task_df['worker_committed'] == worker_hash]['time_worker_start'].mean()
        rows.append(row)

    worker_summary_df = pd.DataFrame(rows)
    worker_summary_df = worker_summary_df.sort_values(by=['worker_id'], ascending=[True])
    worker_summary_df.to_csv(os.path.join(dirname, 'worker_summary.csv'), index=False)



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="This script is used to plot a figure for a transaction log")
    parser.add_argument('--log-dir', type=str, default='most-recent')
    parser.add_argument('--print', action='store_true')
    args = parser.parse_args()

    log_dir = args.log_dir
    data_dir = os.path.join(log_dir, 'vine-logs')

    generate_log_data(log_dir)