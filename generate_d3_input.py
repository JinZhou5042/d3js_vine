import sys
import argparse
import os
import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


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
                task_info[obj_id]['start_time'] = timestamp
                task_info[obj_id]['worker'] = info.split()[0]
            if status == 'WAITING_RETRIEVAL':
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
    # 加载数据
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
    # 对运行时间进行对数变换
    df['Log Run Time'] = np.log1p(df['Run Time'])
    # 合并worker和slot信息为一个新列，用于绘图
    df['Worker-Slot'] = df['Worker'] + '-' + df['Slot']
    return df

def draw_violin_plot(data, x, y, title, filename, mode='svg'):
    plt.figure(figsize=(12, 8))
    sns.violinplot(x=x, y=y, data=data, inner='point')
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel('Run Time (s)')

    # 对y轴刻度进行自定义处理，这里假设y轴是对数变换后的数据
    yticks = plt.gca().get_yticks()
    yticklabels = [f"{np.expm1(val):.2f}" for val in yticks]
    plt.gca().set_yticks(yticks)
    plt.gca().set_yticklabels(yticklabels)
    
    ax = plt.gca()
    if x == 'Worker' and len(data[x].unique()) > 32:
        ax.xaxis.set_major_locator(plt.MaxNLocator(nbins=32))  # 当Worker的唯一值多于32时，限制最大刻度数量
    
    plt.tight_layout()
    if mode == 'svg':
        plt.savefig(filename, format=mode)
    else:
        plt.savefig(filename)
    plt.close()

def gen_violins(mode='svg'):
    df = load_and_preprocess_data("data.json")
    df = transform_data(df)
    df['Worker'] = df['Worker'].str.extract('(\d+)').astype(int)

    unique_workers = df['Worker'].unique()

    # 绘制每个worker的slot小提琴图
    for worker in unique_workers:
        worker_data = df[df['Worker'] == worker]
        filename = os.path.join('Input', f'worker{worker}_violin.{mode}')
        draw_violin_plot(worker_data, 'Slot', 'Log Run Time', f'workek{worker} violin plot', filename, mode=mode)

    filename = os.path.join('Input', f'all_workers_summary_violin.{mode}')
    draw_violin_plot(df, 'Worker', 'Log Run Time', 'All Workers Summary violin plot', filename, mode=mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="This script is used to plot a figure for a transaction log")
    parser.add_argument('--txn-path', type=str, default='most-recent/vine-logs/transactions')
    parser.add_argument('--print', action='store_true')
    args = parser.parse_args()

    # ger information of different catagories from transactions log
    task_info, worker_info, library_info, manager_info = parse_txn_log(args.txn_path)
    # specify the task and library running on a each worker
    match_tasks_to_workers(task_info, worker_info)
    match_libraries_to_workers(library_info, worker_info)
    # plot the running status on workers
    map_tasks_to_slots(worker_info)
    # save the worker information to a json file
    with open("data.json", 'w') as f:
        json.dump(worker_info, f, indent=4)
    gen_violins(mode='svg')
