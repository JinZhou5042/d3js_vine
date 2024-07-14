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

task_info, task_try_count, library_info, worker_info, manager_info, file_info = {}, {}, {}, {}, {}, {}

def remove_invalid_workers():
    print(f"Removing invalid workers...")
    global worker_info, task_info, library_info

    num_total_workers = len(worker_info)
    active_workers = set()
    for task in task_info.values():
        active_workers.add(task['worker_committed'])
    worker_info = {worker_hash: worker for worker_hash, worker in worker_info.items() if worker_hash in active_workers}
    num_active_workers = len(worker_info)
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

    return num_total_workers, num_active_workers

def generate_general_statistics(task_df, worker_summary_df, manager_info, num_total_workers, num_active_workers, dirname):
    #####################################################
    # General Statistics
    print("Generating general_statistics.csv...")
    general_statistics_task_df = task_df.groupby('category').agg({
        'task_id': 'nunique',
        'when_ready': lambda x: (x > 0).sum(),
        'when_running': lambda x: (x > 0).sum(),
        'when_waiting_retrieval': lambda x: (x > 0).sum(),
        'when_retrieved': lambda x: (x > 0).sum(),
        'when_done': lambda x: (x > 0).sum(),
        'worker_id': 'nunique',
    }).rename(columns={
        'task_id': 'submitted',
        'when_ready': 'ready',
        'when_running': 'running',
        'when_waiting_retrieval': 'waiting_retrieval',
        'when_retrieved': 'retrieved',
        'when_done': 'done',
        'worker_id': 'workers',
    }).reset_index()
    total_df = pd.DataFrame(columns=general_statistics_task_df.columns)
    total_df.loc[0, 'category'] = 'TOTAL'
    for col in ['submitted', 'ready', 'running', 'waiting_retrieval', 'retrieved', 'done']:
        total_df.loc[0, col] = general_statistics_task_df[col].sum()
    total_df.loc[0, 'workers'] = task_df['worker_id'].nunique()
    general_statistics_task_df = pd.concat([general_statistics_task_df, total_df], ignore_index=True)
    general_statistics_task_df = general_statistics_task_df.sort_values('submitted', ascending=False)
    general_statistics_task_df.to_csv(os.path.join(dirname, 'general_statistics_task.csv'), index=False)

    general_statistics_worker_df = pd.DataFrame(worker_summary_df)
    # convert time_connected and time_disconnected to datetime
    general_statistics_worker_df['time_connected'] = general_statistics_worker_df['time_connected'].apply(int)
    general_statistics_worker_df['time_disconnected'] = general_statistics_worker_df['time_disconnected'].apply(int)
    general_statistics_worker_df['time_connected'] = pd.to_datetime(general_statistics_worker_df['time_connected'], unit='s')
    general_statistics_worker_df['time_disconnected'] = pd.to_datetime(general_statistics_worker_df['time_disconnected'], unit='s')
    # round the values
    general_statistics_worker_df[['avg_task_runtime(s)', 'peak_disk_usage(MB)', 'peak_disk_usage(%)', 'lifetime(s)']] = general_statistics_worker_df[['avg_task_runtime(s)', 'peak_disk_usage(MB)', 'peak_disk_usage(%)', 'lifetime(s)']].round(2)
    general_statistics_worker_df.to_csv(os.path.join(dirname, 'general_statistics_worker.csv'), index=False)
    #####################################################

    #####################################################
    # Add info into manager_info
    print("Generating general_statistics_manager.csv...")
    manager_info['total_workers'] = num_total_workers
    manager_info['active_workers'] = num_active_workers

    events = pd.concat([
        pd.DataFrame({'time': worker_summary_df['time_connected'], 'type': 'connect', 'worker_id': worker_summary_df['worker_id']}),
        pd.DataFrame({'time': worker_summary_df['time_disconnected'], 'type': 'disconnect', 'worker_id': worker_summary_df['worker_id']})
    ])

    events = events.sort_values('time')
    parallel_workers = 0
    worker_connection_events = []

    worker_connection_events.append((manager_info['time_start'], 0, 'manager_start', -1))
    for _, event in events.iterrows():
        if event['type'] == 'connect':
            parallel_workers += 1
        else:
            parallel_workers -= 1
        worker_connection_events.append((event['time'], parallel_workers, event['type'], event['worker_id']))

    manager_info['max_concurrent_workers'] = max([x[1] for x in worker_connection_events])
    row_task_total = general_statistics_task_df[general_statistics_task_df['category'] == 'TOTAL']
    manager_info['tasks_submitted'] = row_task_total['submitted'].iloc[0]
    manager_info['time_start_human'] = pd.to_datetime(int(manager_info['time_start']), unit='s').strftime('%Y-%m-%d %H:%M:%S')
    manager_info['time_end_human'] = pd.to_datetime(int(manager_info['time_end']), unit='s').strftime('%Y-%m-%d %H:%M:%S')
    worker_connection_events_df = pd.DataFrame(worker_connection_events, columns=['time', 'parallel_workers', 'event', 'worker_id'])
    worker_connection_events_df.to_csv(os.path.join(dirname, 'worker_connections.csv'), index=False)
    # the max try_id in task_df
    manager_info['max_task_try_count'] = task_df['try_id'].max()
    manager_info_df = pd.DataFrame([manager_info])
    manager_info_df.to_csv(os.path.join(dirname, 'general_statistics_manager.csv'), index=False)
    #####################################################

def generate_library_summary(library_info, dirname):
    library_df = pd.DataFrame.from_dict(library_info, orient='index')
    library_df.to_csv(os.path.join(dirname, 'library_summary.csv'), index=False)

def generate_worker_summary(task_df, worker_disk_usage_df, dirname):
    global worker_info, manager_info

    print("Generating worker_summary.csv...")
    rows = []
    for worker_hash, info in worker_info.items():
        row = {
            'worker_id': info['worker_id'],
            'worker_hash': worker_hash,
            'worker_machine_name': info['worker_machine_name'],
            'worker_ip': info['worker_ip'],
            'worker_port': info['worker_port'],
            'time_connected': info['time_connected'],
            'time_disconnected': info['time_disconnected'],
            'lifetime(s)': 0,
            'cores': info['cores'],
            'memory(MB)': info['memory(MB)'],
            'disk(MB)': info['disk(MB)'],
            'tasks_done': 0,
            'avg_task_runtime(s)': 0,
            'peak_disk_usage(MB)': 0,
            'peak_disk_usage(%)': 0,
        }
        # calculate the number of tasks done by this worker
        tasks_committed = task_df[task_df['worker_committed'] == worker_hash]
        tasks_done = tasks_committed[tasks_committed['when_done'] > 0]
        row['tasks_done'] = len(tasks_done)
        # check if this worker has any disk updates
        if not worker_disk_usage_df.empty and worker_disk_usage_df['worker_hash'].isin([worker_hash]).any():
            row['peak_disk_usage(MB)'] = worker_disk_usage_df[worker_disk_usage_df['worker_hash'] == worker_hash]['disk_usage(MB)'].max()
            row['peak_disk_usage(%)'] = worker_disk_usage_df[worker_disk_usage_df['worker_hash'] == worker_hash]['disk_usage(%)'].max()
        # the worker may not have any tasks
        if row['tasks_done'] > 0:
            row['avg_task_runtime(s)'] = task_df[task_df['worker_committed'] == worker_hash]['time_worker_end'].mean() - task_df[task_df['worker_committed'] == worker_hash]['time_worker_start'].mean()
        if len(info['time_connected']) != len(info['time_disconnected']):
            info['time_disconnected'].append(manager_info['time_end'])
            # raise ValueError("time_connected and time_disconnected have different lengths.")
        for i in range(len(info['time_connected'])):
            row_copy = copy.deepcopy(row)
            row_copy['time_connected'] = info['time_connected'][i]
            row_copy['time_disconnected'] = info['time_disconnected'][i]
            row_copy['lifetime(s)'] = info['time_disconnected'][i] - info['time_connected'][i]
            rows.append(row_copy)

    worker_summary_df = pd.DataFrame(rows)
    worker_summary_df = worker_summary_df.sort_values(by=['worker_id'], ascending=[True])
    worker_summary_df.to_csv(os.path.join(dirname, 'worker_summary.csv'), index=False)

    return worker_summary_df

def handle_task_info(dirname):
    global task_info, task_try_count, manager_info, file_info
    print("Generating task.csv...")

    task_df = pd.DataFrame.from_dict(task_info, orient='index')
    # ensure that the running time is not greater than the done time
    task_df['when_running'] = np.where(
        task_df['time_worker_start'].gt(0) & task_df['time_worker_start'].notna(),
        np.minimum(task_df['when_running'], task_df['time_worker_start']),
        task_df['when_running']
    )
    
    task_df.to_csv(os.path.join(dirname, 'task.csv'), index=False)

    is_done = task_df['when_done'].notnull()
    is_failed_manager = task_df['when_running'].isnull() & task_df['when_ready'].notnull()
    is_failed_worker = task_df['when_running'].notnull() & task_df['when_done'].isnull()

    manager_info['tasks_done'] = is_done.sum()
    manager_info['tasks_failed_on_manager'] = is_failed_manager.sum()
    manager_info['tasks_failed_on_worker'] = is_failed_worker.sum()

    def calculate_total_size_of_files(files):
        return round(sum([file_info[file]['size(MB)'] for file in files]), 4)

    def handle_each_task(task):
        # assume that every task consumes 1 core as of now
        cores = task['core_id']
        if len(cores) == 0:
            return task
        task['core_id'] = cores[0]
        # if the when_next_ready is na, that means the manager exited before the task was ready, set it to the end time
        if pd.isna(task['when_next_ready']):
            task['when_next_ready'] = manager_info['time_end']
        # calculate the total size of input and output files
        task['size_input_files(MB)'] = calculate_total_size_of_files(task['input_files'])
        task['size_output_files(MB)'] = calculate_total_size_of_files(task['output_files'])
        # calculate the critical parent
        parents = []
        for input_file in task['input_files']:
            file_producers = file_info[input_file]['producers']
            if file_producers:
                parents.extend(file_producers)
        # find the critical input file
        shorted_waiting_time = 1e8
        for p in parents:
            # exclude recovery tasks
            # if task['is_recovery_task']:
            #    continue
            parent_task = task_info[(p, task_try_count[p])]
            task_start_timestamp = 'time_worker_start'
            task_finish_timestamp = 'time_worker_end'
            time_period = task[task_start_timestamp] - parent_task[task_finish_timestamp]

            if time_period < 0:
                # it means that this input file is lost after this task is done
                # and it is used as another task's input file
                continue
            if time_period < shorted_waiting_time:
                shorted_waiting_time = task[task_start_timestamp] - parent_task[task_finish_timestamp]
                task['critical_parent'] = int(p)
                task['critical_input_file'] = parent_task['output_files'][0]
                task['critical_input_file_wait_time'] = shorted_waiting_time

        return task

    task_df[is_done].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_done.csv'), index=False)
    task_df[is_failed_manager].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_failed_on_manager.csv'), index=False)
    task_df[is_failed_worker].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_failed_on_worker.csv'), index=False)

    return task_df

def generate_worker_disk_usage_df(worker_info, dirname):
    print("Generating worker_disk_usage.csv...")
    rows = []
    for worker_hash, worker in worker_info.items():
        worker_id = worker['worker_id']
        for filename, disk_update in worker['disk_update'].items():
            # Initial checks for disk update logs
            len_in = len(disk_update['when_stage_in'])
            len_out = len(disk_update['when_stage_out'])

            # Preparing row data
            for time, disk_increment in zip(disk_update['when_stage_in'] + disk_update['when_stage_out'],
                                                 [disk_update['size(MB)']] * len_in + [-disk_update['size(MB)']] * len_out):
                rows.append({
                    'worker_hash': worker_hash,
                    'worker_id': worker_id,
                    'filename': filename,
                    'time': time,
                    'size(MB)': disk_increment
                })

    worker_disk_usage_df = pd.DataFrame(rows)

    if not worker_disk_usage_df.empty:
        worker_disk_usage_df = worker_disk_usage_df[worker_disk_usage_df['time'] > 0]
        worker_disk_usage_df.sort_values(by=['worker_id', 'time'], ascending=[True, True], inplace=True)
        # normal worker disk usage
        worker_disk_usage_df['disk_usage(MB)'] = worker_disk_usage_df.groupby('worker_id')['size(MB)'].cumsum()
        worker_disk_usage_df['disk_usage(%)'] = worker_disk_usage_df['disk_usage(MB)'] / worker_disk_usage_df['worker_hash'].map(lambda x: worker_info[x]['disk(MB)'])
        # only consider the accumulated disk usage (exclude the stage-out files)
        worker_disk_usage_df['positive_size(MB)'] = worker_disk_usage_df['size(MB)'].apply(lambda x: x if x > 0 else 0)
        worker_disk_usage_df['disk_usage_accumulation(MB)'] = worker_disk_usage_df.groupby('worker_id')['positive_size(MB)'].cumsum()
        worker_disk_usage_df['disk_usage_accumulation(%)'] = worker_disk_usage_df['disk_usage_accumulation(MB)'] / worker_disk_usage_df['worker_hash'].map(lambda x: worker_info[x]['disk(MB)'])
        worker_disk_usage_df.drop('positive_size(MB)', axis=1, inplace=True)

        worker_disk_usage_df.to_csv(os.path.join(dirname, 'worker_disk_usage.csv'), index=False)

    return worker_disk_usage_df

def handle_file_info(dirname):
    print(f"Generating file_info.csv...")
    global file_info, worker_info

    data = []
    for filename, info in file_info.items():
        cleaned_records = []
        for record in info['worker_holding']:
            # an inactive worker, skip
            if record['worker_hash'] not in worker_info:
                continue
            worker_id = worker_info[record['worker_hash']]['worker_id']
            time_stage_in = round(record['time_stage_in'], 2)
            time_stage_out = round(record['time_stage_out'], 2)
            life_time = round(time_stage_out - time_stage_in, 2)
            cleaned_records.append([worker_id, time_stage_in, time_stage_out, life_time])
        cleaned_records.sort(key=lambda x: x[1])
        info['num_workers_holding'] = len(info['worker_holding'])
        del info['worker_holding']
        info['worker_holding'] = cleaned_records
        # remove files that are not produced by any task
        if not info['producers']:
            continue
        data.append({
            'filename': filename,
            'size(MB)': info['size(MB)'],
            'num_worker_holding': len(info['worker_holding']),
            'producers': info['producers'],
            'consumers': info['consumers']
        })
    # save the file_info into a csv file, should use filename as key
    file_info_df = pd.DataFrame.from_dict(file_info, orient='index')
    file_info_df.index.name = 'filename'
    file_info_df.to_csv(os.path.join(dirname, 'file_info.csv'))

    file_info_df = pd.DataFrame(data)
    file_info_df.to_csv(os.path.join(dirname, 'general_statistics_file.csv'), index=False)
    return file_info_df

def generate_data(log_dir):
    global task_info, task_try_count, library_info, worker_info, manager_info, file_info

    dirname = os.path.join(log_dir, 'vine-logs')
    txn = os.path.join(dirname, 'transactions')
    debug = os.path.join(dirname, 'debug')
    taskgraph = os.path.join(dirname, 'taskgraph')

    task_info, task_try_count, library_info, worker_info, manager_info = parse_txn(txn)
    worker_info, file_info = parse_debug(debug, worker_info, task_info, task_try_count, manager_info)
    task_info = parse_taskgraph(taskgraph, task_info, task_try_count, file_info)

    num_total_workers, num_active_workers = remove_invalid_workers()

    handle_file_info(dirname)

    task_df = handle_task_info(dirname)

    with open(os.path.join(dirname, 'worker_info.json'), 'w') as f:
        json.dump(worker_info, f, indent=4)

    generate_library_summary(library_info, dirname)

    worker_disk_usage_df  = generate_worker_disk_usage_df(worker_info, dirname)
 
    worker_summary_df = generate_worker_summary(task_df, worker_disk_usage_df, dirname)
    
    generate_general_statistics(task_df, worker_summary_df, manager_info, num_total_workers, num_active_workers, dirname)

   
if __name__ == '__main__':

    # parser = argparse.ArgumentParser(description="This script is used to plot a figure for a transaction log")
    # parser.add_argument('--log-dir', type=str, default='most-recent')
    # parser.add_argument('--print', action='store_true')
    # args = parser.parse_args()
    # log_dir = args.log_dir

    if len(sys.argv) > 1:
        log_dir = sys.argv[1]
    else:
        with os.scandir('logs') as entries:
            for entry in sorted(entries, key=lambda e: e.name):
                if entry.is_dir():
                    log_dir = os.path.join('logs', entry.name)
                    print(log_dir)
                    break

    data_dir = os.path.join(log_dir, 'vine-logs')

    generate_data(log_dir)