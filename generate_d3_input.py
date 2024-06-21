import sys
import argparse
import os
import copy
import json
from parse_logs import parse_txn, parse_taskgraph, parse_debug
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
import ast
import numpy as np


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
    print("Generating task.csv...")
    task_df = pd.DataFrame.from_dict(task_info, orient='index')
    # in some cases, the when_running is a little bit larger than time_worker_start
    mask = task_df['time_worker_start'].gt(0) & task_df['time_worker_start'].notna()
    task_df.loc[mask, 'when_running'] = np.minimum(task_df.loc[mask, 'when_running'], task_df.loc[mask, 'time_worker_start'])
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

def generate_data(log_dir):

    dirname = os.path.join(log_dir, 'vine-logs')
    txn = os.path.join(dirname, 'transactions')
    debug = os.path.join(dirname, 'debug')
    taskgraph = os.path.join(dirname, 'taskgraph')

    task_info, task_try_count, library_info, worker_info, manager_info = parse_txn(txn)
    task_info = parse_taskgraph(taskgraph, task_info, task_try_count)
    worker_info = parse_debug(debug, worker_info, task_info, task_try_count)
    
    #####################################################
    # Remove invalid workers: workers didn't commit any task
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
        for filename, disk_update in worker['disk_update'].items():
            row = {
                'worker_hash': worker_hash,
                'worker_id': worker['worker_id'],
                'filename': filename,
                'time': None,
                'size(MB)': 0,
            }
            if len(disk_update['when_stage_in']) < len(disk_update['when_stage_out']):
                print(f"Warning: worker {worker_hash} has more stage-outs than stage-ins on file {filename}.")
            if len(disk_update['when_stage_in']) > len(disk_update['when_stage_out']):
                # manually add a stage-out at the end of the log
                when_stage_in = disk_update['when_stage_in'][-1]
                worker_connected_id = 0
                for worker_connected in worker['time_connected']:
                    if worker_connected < when_stage_in and worker['time_disconnected'][worker_connected_id] > when_stage_in:
                        disk_update['when_stage_out'].append(worker['time_disconnected'][worker_connected_id])
                        break

            for time_stage_in in disk_update['when_stage_in']:
                row_copy = copy.deepcopy(row)
                row_copy['time'] = time_stage_in
                row_copy['size(MB)'] = disk_update['size(MB)']
                rows.append(row_copy)
            for time_stage_out in disk_update['when_stage_out']:
                row_copy = copy.deepcopy(row)
                row_copy['time'] = time_stage_out
                row_copy['size(MB)'] = -disk_update['size(MB)']
                rows.append(row_copy)
    disk_update_df = pd.DataFrame(rows)

    # disk_update_df may be empty
    if not disk_update_df.empty:
        disk_update_df = disk_update_df[disk_update_df['time'] > 0]
        disk_update_df.sort_values(by=['worker_id', 'time'], ascending=[True, True], inplace=True)
        disk_update_df['disk_usage(MB)'] = disk_update_df.groupby('worker_id')['size(MB)'].cumsum()
        disk_update_df['disk_usage(%)'] = disk_update_df.apply(lambda x: x['disk_usage(MB)'] / worker_info[x['worker_hash']]['disk(MB)'], axis=1)
        disk_update_df.to_csv(os.path.join(dirname, 'worker_disk_update.csv'), index=False)

    # convert worker_info to DataFrame
    print("Generating worker_summary.csv...")
    rows = []
    for worker_hash, info in worker_info.items():
        row = {
            'worker_hash': worker_hash,
            'worker_id': info['worker_id'],
            'worker_machine_name': info['worker_machine_name'],
            'worker_ip': info['worker_ip'],
            'worker_port': info['worker_port'],
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
            row['peak_disk_usage(%)'] = disk_update_df[disk_update_df['worker_hash'] == worker_hash]['disk_usage(%)'].max()
        # the worker may not have any tasks
        if row['tasks_done'] > 0:
            row['avg_task_runtime(s)'] = task_df[task_df['worker_committed'] == worker_hash]['time_worker_end'].mean() - task_df[task_df['worker_committed'] == worker_hash]['time_worker_start'].mean()
        if len(info['time_connected']) != len(info['time_disconnected']):
            raise ValueError("time_connected and time_disconnected have different lengths.")
        for i in range(len(info['time_connected'])):
            row_copy = copy.deepcopy(row)
            row_copy['time_connected'] = info['time_connected'][i]
            row_copy['time_disconnected'] = info['time_disconnected'][i]
            rows.append(row_copy)

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

    generate_data(log_dir)