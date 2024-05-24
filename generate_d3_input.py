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


def get_runtime(txn):
    """
    Processes a log file, extracting and organizing task, worker, and library information. 
    Tracks the time the first task was ran, and associates tasks and libraries with workers.
    """
    log_lines = open(txn, 'r').read().splitlines()
    
    task_info = {}
    worker_info = {}
    library_info = {}

    for line in log_lines:
        if line.startswith("#"):
            continue
        
        timestamp, _, category, obj_id, status, info = line.split(maxsplit=5)
        try:
            timestamp = float(timestamp) / 1000000
        except ValueError:
            continue

        if category == 'TASK':
            if obj_id not in task_info:
                task_info[obj_id] = {}
            if status == 'READY':
                function = info.split()[0]
                task_info[obj_id]['dispatch_time'] = timestamp
                task_info[obj_id]['function'] = function
            if status == 'RUNNING':
                task_info[obj_id]['task_id'] = obj_id
                task_info[obj_id]['start_time'] = timestamp
                task_info[obj_id]['worker'] = info.split()[0]
            if status == 'WAITING_RETRIEVAL':
                task_info[obj_id]['stop_time'] = timestamp
        if category == 'WORKER':
            if obj_id not in worker_info:
                worker_info[obj_id] = {'tasks': [], 'libraries': []}
            if status == 'CONNECTION':
                worker_info[obj_id]['start_time'] = timestamp
            if status == 'DISCONNECTION':
                worker_info[obj_id]['stop_time'] = timestamp
        if category == 'LIBRARY':
            if obj_id not in library_info:
                library_info[obj_id] = {}
            if status == 'STARTED':
                library_info[obj_id]['start_time'] = timestamp
                library_info[obj_id]['worker'] = info

    for task in task_info:
        worker_id = task_info[task].get('worker')
        if worker_id in worker_info and 'stop_time' in task_info[task]:
            worker_info[worker_id]['tasks'].append(task_info[task])
    for library in library_info:
        worker_id = library_info[library].get('worker')
        if worker_id in worker_info:
            worker_info[worker_id]['libraries'].append(library_info[library])
    first_task_start = float('inf')
    first_task_dispatch = float('inf')
    last_task_finish = float(0)
    for worker in worker_info:
        for task in worker_info[worker]['tasks']:
            first_task_dispatch = min(task['dispatch_time'], first_task_dispatch)
            first_task_start = min(task['start_time'], first_task_start)
            last_task_finish = max(last_task_finish, task['stop_time'])

    return last_task_finish - first_task_start


def parse_txn_log(txn):
    """
    Processes a log file, extracting and organizing task, worker, and library information. 
    Tracks the time the first task was ran, and associates tasks and libraries with workers.
    """
    log_lines = open(txn, 'r').read().splitlines()
    
    task_info = {}
    worker_info = {}
    library_info = {}
    manager_info = {}

    for line in log_lines:
        if line.startswith("#"):
            continue
        
        timestamp, _, category, obj_id, status, info = line.split(maxsplit=5)
        try:
            timestamp = float(timestamp) / 1000000
        except ValueError:
            continue

        if category == 'TASK':
            if obj_id not in task_info:
                task_info[obj_id] = {}
            if status == 'READY':
                function = info.split()[0]
                task_info[obj_id]['dispatch_time'] = timestamp
                task_info[obj_id]['function'] = function
            if status == 'RUNNING':
                if 'dispatch_time' not in task_info[obj_id]:
                    continue
                task_info[obj_id]['start_time'] = timestamp
                task_info[obj_id]['worker'] = info.split()[0]
            if status == 'WAITING_RETRIEVAL':
                if 'dispatch_time' not in task_info[obj_id]:
                    continue
                task_info[obj_id]['stop_time'] = timestamp
        if category == 'WORKER':
            if not obj_id.startswith('worker'):
                continue
            if obj_id not in worker_info:
                worker_info[obj_id] = {'tasks': [], 'libraries': []}
            if status == 'CONNECTION':
                worker_info[obj_id]['start_time'] = timestamp
            if status == 'DISCONNECTION':
                worker_info[obj_id]['stop_time'] = timestamp
        if category == 'LIBRARY':
            if obj_id not in library_info:
                library_info[obj_id] = {}
            if status == 'STARTED':
                library_info[obj_id]['start_time'] = timestamp
                library_info[obj_id]['worker'] = info
        if category == 'MANAGER':
            if status == 'START':
                manager_info['start_time'] = timestamp
            if status == 'END':
                manager_info['end_time'] = timestamp

    # The stop time for the manager is not recorded in the log, so we set it to the end time of the library task
    for library in library_info:
        if 'stop_time' not in library_info[library]:
            library_info[library]['stop_time'] = manager_info['end_time']

    return task_info, worker_info, library_info, manager_info


def match_tasks_to_workers(task_info, worker_info):
    """
    Matches each task with its responsible worker by adding the task to the respective worker's task list in the 'workers' dictionary.
    Only tasks that have a 'stop_time' recorded (indicating completion or termination) are considered for matching.
    """
    for task in task_info:
        worker_id = task_info[task].get('worker')
        if worker_id in worker_info and 'stop_time' in task_info[task]:
            worker_info[worker_id]['tasks'].append(task_info[task])

def match_libraries_to_workers(library_info, worker_info):
    """
    Assigns each library to its managing worker by adding the library details to the corresponding worker's list of libraries in the 'workers' dictionary.
    """
    for library in library_info:
        worker_id = library_info[library].get('worker')
        if worker_id in worker_info:
            worker_info[worker_id]['libraries'].append(library_info[library])


def map_tasks_to_slots(worker_info):
    first_task_start = float('inf')
    first_task_dispatch = float('inf')
    for worker in worker_info:
        for task in worker_info[worker]['tasks']:
            first_task_dispatch = min(task['dispatch_time'], first_task_dispatch)
            first_task_start = min(task['start_time'], first_task_start)
    
    total_task_count = 0

    def add_task_to_slots(slots, task):
        for slot_tasks in slots.values():
            if task[1] > slot_tasks[-1][2]:
                slot_tasks.append(task)
                return True
        return False

    for worker, info in worker_info.items():
        slots = {}
        tasks = sorted(
            [[task['dispatch_time'], task['start_time'], task['stop_time'], task['function']] for task in info['tasks']],
            key=lambda x: x[1]
        )
        total_task_count += len(tasks)

        for task in tasks:
            if not slots:
                slots[1] = [task]
            else:
                if not add_task_to_slots(slots, task):
                    slots[len(slots) + 1] = [task]

        for library_id, library in enumerate(info['libraries']):
            # for libraries, assume the dispatch time is the start time
            slots[len(slots) + 1] = [[library['start_time'], library['start_time'], library['stop_time'], f'library_{library_id}']]

        worker_info[worker]['slots'] = slots

        del worker_info[worker]["tasks"]
        del worker_info[worker]["libraries"]

def load_and_preprocess_data(filepath):
    # load worker info from json file
    with open(filepath, 'r') as f:
        worker_info = json.load(f)

    # 创建worker到简化标号的映射
    worker_mapping = {worker_id: f"worker{i+1}" for i, worker_id in enumerate(worker_info.keys())}

    # 初始化数据收集列表
    task_data = []

    for worker_id, worker in worker_info.items():
        worker_short_id = worker_mapping[worker_id]  # 使用简化标号
        for slot_id, slot_tasks in worker["slots"].items():
            for task in slot_tasks:
                if task[3].startswith('library'):
                    continue
                start_time, stop_time = task[1], task[2]
                run_time = stop_time - start_time
                task_data.append({
                    'Worker': worker_short_id,
                    'Slot': f"slot{slot_id}",
                    'Run Time': run_time
                })

    return pd.DataFrame(task_data)

def transform_data(df):
    # calculate the log of run time
    df['Log Run Time'] = np.log1p(df['Run Time'])
    # combine worker and slot to form a unique identifier
    df['Worker-Slot'] = df['Worker'] + '-' + df['Slot']
    return df

def draw_violin_plot(data, x, y, title, filename, mode='svg', display=False):

    plt.figure(figsize=(12, 8))
    sns.violinplot(x=x, y=y, data=data, inner='point')
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel('Run Time (s)')

    # set yticks to be the original run time values
    yticks = plt.gca().get_yticks()
    yticklabels = [f"{np.expm1(val):.2f}" for val in yticks]
    plt.gca().set_yticks(yticks)
    plt.gca().set_yticklabels(yticklabels)
    
    ax = plt.gca()
    if len(data[x].unique()) > 16:
        ax.xaxis.set_major_locator(plt.MaxNLocator(nbins=16))
    
    plt.tight_layout()
    if mode == 'svg':
        plt.savefig(filename, format=mode)
    else:
        plt.savefig(filename)

    if display:
        plt.show()
    plt.close()


def gen_violins(data_dir, mode='svg', display=False):
    df = load_and_preprocess_data(os.path.join(data_dir, 'worker_tasks.json'))
    df = transform_data(df)
    df['Worker'] = df['Worker'].str.extract('(\d+)').astype(int)
    df['Slot'] = df['Slot'].str.extract('(\d+)').astype(int)

    unique_workers = df['Worker'].unique()

    # draw the violin plot for each worker
    for worker in unique_workers:
        worker_data = df[df['Worker'] == worker]
        filename = os.path.join(data_dir, f'worker{worker}_violin.{mode}')
        draw_violin_plot(worker_data, 'Slot', 'Log Run Time', f'workek{worker} violin plot', filename, mode=mode, display=False)

    # draw the violin plot for all workers
    filename = os.path.join(data_dir, f'all_workers_summary_violin.{mode}')
    draw_violin_plot(df, 'Worker', 'Log Run Time', 'All Workers Summary violin plot', filename, mode=mode, display=display)


def init_data_dir(data_dir):
    if os.path.exists(data_dir) and os.path.isdir(data_dir):
        shutil.rmtree(data_dir)
        os.makedirs(data_dir)
    else:
        os.makedirs(data_dir)


def generate_description(log_dir, worker_info):
    data_dir = os.path.join(log_dir, 'vine-logs')
    txn = os.path.join(log_dir, 'vine-logs', 'transactions')
    app_info = {
        "log_dir": log_dir.split('/')[-1],
        "total_tasks": 0,
        "total_workers": len(worker_info),
        "average_execution_time": 0,
    }
    total_tasks = 0
    total_workers = len(worker_info)
    total_execution_time = 0
    first_task_start = float('inf')
    first_task_dispatch = float('inf')
    last_task_finish = float(0)
    for worker_id, worker_data in worker_info.items():
        for slot_id, tasks in worker_data["slots"].items():
            for task in tasks:
                # 解析任务数据
                dispatch_time, start_time, end_time, task_type = task
                if task_type.startswith('library'):
                    continue
                # 更新首个任务开始时间
                first_task_dispatch = min(dispatch_time, first_task_dispatch)
                first_task_start = min(start_time, first_task_start)
                last_task_finish = max(last_task_finish, end_time)
                # 累计任务数量
                total_tasks += 1
                # 累计执行时间
                total_execution_time += (end_time - start_time)

    # 计算平均执行时间
    average_execution_time = total_execution_time / total_tasks if total_tasks else 0
    app_runtime = last_task_finish - first_task_start

    app_info["total_tasks"] = total_tasks
    app_info["average_execution_time"] = str(round(average_execution_time, 4)) + "s"
    app_info["total_execution_time"] = str(round(total_execution_time, 4)) + "s"
    app_info["app_runtime"] = str(round(app_runtime, 4)) + "s"

    with open(os.path.join(data_dir, 'app_info.json'), 'w') as f:
        json.dump(app_info, f, indent=4)


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


def generate_core_load_plots(log_dir, data_dir, mode='png'):
    # load the data
    resource_csv = os.path.join(log_dir, 'vine-logs', 'resource_consumption_report.csv')
    worker_config_csv = os.path.join(log_dir, 'vine-logs', 'workerConfigs.csv')
    if not os.path.exists(resource_csv) or not os.path.exists(worker_config_csv):
        print("Resource consumption report or worker configs not found.")
        return
    
    # in case it is empty
    try:
        resource_consumption_df = pd.read_csv(resource_csv)
    except pd.errors.EmptyDataError:
        print("Resource consumption report is empty.")
        return
    try:
        worker_configs_df = pd.read_csv(worker_config_csv)
    except pd.errors.EmptyDataError:
        print("Worker configs is empty.")
        return

    # combine the two dataframes
    columns_to_merge = ['hostname', 'ip', 'port', 'worker_hash', 'function_slots', 'cores', 'memory', 'disk', 'gpus']
    # check whether the columns exist in the dataframe

    worker_configs_subset = worker_configs_df[columns_to_merge]
    combined_df = pd.merge(resource_consumption_df, worker_configs_subset, on='hostname', how='left', suffixes=('_x', '_y'))
    combined_df.sort_values('timestamp', inplace=True)
    combined_df['relative_timestamp'] = (combined_df['timestamp'] - combined_df['timestamp'].min())
    combined_df.drop(columns=[col for col in combined_df if col.endswith('_y')], inplace=True)
    combined_df.rename(columns=lambda x: x.rstrip('_x'), inplace=True)

    combined_df = combined_df[combined_df['cores'] != 0]

    combined_df['avg_core_load_each_task'] = combined_df['user_total_cpu_usage'] / combined_df['user_concurrent_tasks']

    sorted_hostnames = worker_configs_df.sort_values('worker_hash')['hostname'].unique()
    
    # plot individual cpu load on allocated cores
    for i, hostname in enumerate(sorted_hostnames, start=1):
        group = combined_df[combined_df['hostname'] == hostname]
        if not group.empty:
            fig, ax1 = plt.subplots(figsize=(12, 8))
            # cpu load plot
            color = '#2b4ead'
            ax1.set_xlabel('Time (s)')
            ax1.set_ylabel('Average CPU Load (%)', color=color)
            ax1.plot(group['relative_timestamp'], group['avg_core_load_each_task'], marker='o', linestyle='-', color=color)
            ax1.tick_params(axis='y', labelcolor=color)
            ax1.grid(True)
            # concurrent tasks plot
            ax2 = ax1.twinx() 
            color = '#005239'
            ax2.set_ylabel('Concurrent Tasks', color=color)
            ax2.scatter(group['relative_timestamp'], group['user_concurrent_tasks'], color=color, alpha=0.8, edgecolors='none', s=12)
            ax2.tick_params(axis='y', labelcolor=color)
            # title
            plt.title(f'Average CPU Load and Concurrent Tasks for worker{i} ({hostname})')
            if mode == 'png':
                plot_filename = os.path.join(data_dir, f'worker{i}_core_load.png')
                plt.savefig(plot_filename)
            elif mode == 'svg':
                plot_filename = os.path.join(data_dir, f'worker{i}_core_load.svg')
                plt.savefig(plot_filename, format='svg')
            
            plt.close(fig)

    # plot overall average cpu load on allocated cores
    plt.figure(figsize=(12, 8))
    plt.plot(combined_df['relative_timestamp'], combined_df['avg_core_load_each_task'], marker='o', linestyle='-', color='purple')
    plt.title('Overall Average CPU Load')
    plt.xlabel('Time (s)')
    plt.ylabel('Average CPU Load (%)')
    plt.grid(True)
    if mode == 'png':
        overall_plot_filename = os.path.join(data_dir, 'overall_core_load.png')
        plt.savefig(overall_plot_filename)
    if mode == 'svg':
        overall_plot_filename = os.path.join(data_dir, 'overall_core_load.svg')
        plt.savefig(overall_plot_filename, format='svg')
    plt.close()

    combined_df.to_csv(resource_csv, index=False)


def generate_log_data(log_dir, data_dir):
    # init_data_dir(data_dir)
    # ger information of different catagories from transactions log
    txn = os.path.join(log_dir, 'vine-logs', 'transactions')

    task_info, worker_info, library_info, manager_info = parse_txn_log(txn)

    # specify the task and library running on a each worker
    match_tasks_to_workers(task_info, worker_info)
    match_libraries_to_workers(library_info, worker_info)
    # plot the running status on workers
    map_tasks_to_slots(worker_info)
    # process logs
    parse_debug(data_dir)
    # generate description of the running status
    generate_description(log_dir, worker_info)

    # sort the worker_info by worker_hash and save it to a json file
    workers_list = list(worker_info.items())
    sorted_workers_list = sorted(workers_list, key=lambda x: x[0])
    for i, (worker_hash, worker_info) in enumerate(sorted_workers_list, start=1):
        sorted_workers_list[i-1] = (worker_hash, worker_info)
    sorted_workers_dict = {worker_hash: worker_info for worker_hash, worker_info in sorted_workers_list}

    worker_info_filename = os.path.join(data_dir, 'worker_tasks.json')
    with open(worker_info_filename, 'w') as f:
        json.dump(sorted_workers_dict, f, indent=4)

    # generate violin plots based on the generated worker_tasks.json
    gen_violins(data_dir, mode='svg')

    # generate cpu load plots
    generate_core_load_plots(log_dir, data_dir, mode='svg')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="This script is used to plot a figure for a transaction log")
    parser.add_argument('--log-dir', type=str, default='most-recent')
    parser.add_argument('--print', action='store_true')
    args = parser.parse_args()

    log_dir = args.log_dir
    data_dir = os.path.join(log_dir, 'vine-logs')

    generate_log_data(log_dir, data_dir)