import sys
import argparse
import os
import json
import matplotlib.pyplot as plt


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


def visualize(worker_info, output_file):
    task_data = {
        'running': {'y_positions': [], 'widths': [], 'start_positions': []},
        'library': {'y_positions': [], 'widths': [], 'start_positions': []},
        'transmitting': {'y_positions': [], 'widths': [], 'start_positions': []}
    }

    y_position = 0

    # calculate first_task_dispatch
    first_task_dispatch = float('inf')
    first_task_start = float('inf')
    for info in worker_info.values():
        for slot_tasks in info["slots"].values():
            for task in slot_tasks:
                first_task_dispatch = min(first_task_dispatch, task[0])
                first_task_start = min(first_task_start, task[1])

    for info in worker_info.values():
        for slot_tasks in info["slots"].values():
            y_position += 1

            for task in slot_tasks:
                if 'random' in task[3]:
                    task_type = 'random'
                elif 'default' in task[3]:
                    task_type = 'running'
                elif 'library' in task[3]:
                    task_type = 'library'
                task_data[task_type]['y_positions'].append(y_position)
                task_data[task_type]['widths'].append(task[2] - task[1])
                task_data[task_type]['start_positions'].append(task[1] - first_task_dispatch)
            task_data['transmitting']['y_positions'].append(y_position)


    if task_data['running']['y_positions']:
        plt.barh(task_data['running']['y_positions'], task_data['running']['widths'], left=task_data['running']['start_positions'], label='Running', color='orange')

    if task_data['library']['y_positions']:
        plt.barh(task_data['library']['y_positions'], task_data['library']['widths'], left=task_data['library']['start_positions'], label='Library', color='green')

    plt.tick_params(axis='y', labelleft=False)
    plt.xlabel('Time (s)')
    plt.xlim([0, None])
    plt.legend()
    plt.savefig(output_file)
    # plt.show()


if __name__ == '__main__':
    default_log_dir = 'most-recent/vine-logs'
    parser = argparse.ArgumentParser(description="This script is used to plot a figure for a transaction log")
    parser.add_argument('--txn_path', type=str, default=default_log_dir)
    parser.add_argument('--txn_name', type=str, default='transactions')
    parser.add_argument('--out_path', type=str, default=default_log_dir)
    parser.add_argument('--out_name', type=str, default='txn.png')
    parser.add_argument('--print', action='store_true')
    args = parser.parse_args()

    txn = os.path.join(args.txn_path, args.txn_name)
    out = os.path.join(args.out_path, args.out_name)

    # ger information of different catagories from transactions log
    task_info, worker_info, library_info, manager_info = parse_txn_log(txn)
    # specify the task and library running on a each worker
    match_tasks_to_workers(task_info, worker_info)
    match_libraries_to_workers(library_info, worker_info)
    # plot the running status on workers
    map_tasks_to_slots(worker_info)
    # save the worker information to a json file
    with open("data.json", 'w') as f:
        json.dump(worker_info, f, indent=4)
    visualize(worker_info, out)
