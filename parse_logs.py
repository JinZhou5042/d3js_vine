import json
from bitarray import bitarray
import time
from tqdm import tqdm
import re
from datetime import datetime
import pytz


def datestring_to_timestamp(datestring):
    eastern = pytz.timezone('US/Eastern')
    date_obj = datetime.strptime(datestring, "%Y/%m/%d %H:%M:%S.%f")
    localized_date = eastern.localize(date_obj)
    unix_timestamp = localized_date.timestamp()
    return unix_timestamp


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
                        'is_recovery_task': False,

                    }
                    if task['cores_requested'] == 0:
                        task['cores_requested'] = 1
                    task_info[(task_id, try_id)] = task
                if status == 'RUNNING':
                    # a running task can be a library which does not have a ready status
                    resources_allocated = json.loads(info.split(' ', 3)[-1])
                    if task_id in task_try_count:
                        try_id = task_try_count[task_id]
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
                    if obj_id not in worker_info:
                        worker_info[obj_id] = {
                            'time_connected': [timestamp],
                            'time_disconnected': [],
                            'worker_id': -1,
                            'worker_machine_name': None,
                            'worker_ip': None,
                            'worker_port': None,
                            'tasks_done': 0,
                            'cores': None,
                            'memory(MB)': None,
                            'disk(MB)': None,
                            'disk_update': {},
                        }
                    else:
                        worker_info[obj_id]['time_connected'].append(timestamp)
                elif status == 'DISCONNECTION':
                    worker_info[obj_id]['time_disconnected'].append(timestamp)
                elif status == 'RESOURCES':
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
                elif status == 'TRANSFER' or status == 'CACHE_UPDATE':
                    if status == 'TRANSFER':
                        # don't consider transfer as of now
                        transfer_type, filename, size_in_bytes, wall_time, start_time = info.split(' ', 4)
                    elif status == 'CACHE_UPDATE':
                        transfer_type = 'CACHE_UPDATE'
                        filename, size_in_bytes, wall_time, start_time = info.split(' ', 3)

                    start_time = float(start_time) / 1e6
                    wall_time = float(wall_time) / 1e6

                    # start time should be after the manager start time
                    if start_time < manager_info['time_start']:
                        # consider xxx.04224 and xxx.0 as the same time
                        if abs(start_time - manager_info['time_start']) < 1:
                            start_time = manager_info['time_start']
                        else:
                            print(f"Warning: {transfer_type} start time {start_time} is before manager start time {manager_info['time_start']}")

                    # update disk usage
                    size_in_bytes = int(size_in_bytes)
                    if filename not in worker_info[obj_id]['disk_update']:
                        # this is the first time the file is cached
                        worker_info[obj_id]["disk_update"][filename] = {
                            'size(MB)': size_in_bytes / 2**20,
                            'when_stage_in': [start_time],
                            'when_stage_out': [],
                        }
                    else:
                        worker_info[obj_id]['disk_update'][filename]['when_stage_in'].append(start_time)

                    # update file_info
                    if filename not in file_info:
                        file_info[filename] = {
                            'size(MB)': size_in_bytes / 2**20,
                            'producers': [],
                            'consumers': [],
                            'worker_holding': [],
                        }

            if category == 'LIBRARY':
                if status == 'SENT':
                    for library in library_info.values():
                        if library['task_id'] == obj_id:
                            library['when_sent'] = timestamp
                if status == 'STARTED':
                    for library in library_info.values():
                        if library['task_id'] == obj_id:
                            library['when_started'] = timestamp
            if category == 'MANAGER':
                if status == 'START':
                    manager_info = {
                        'time_start': timestamp,
                        'time_end': None,
                        'lifetime(s)': None,
                        'time_start_human': None,
                        'time_end_human': None,
                        'tasks_submitted': 0,
                        'tasks_done': 0,
                        'tasks_failed_on_manager': 0,
                        'tasks_failed_on_worker': 0,
                        'max_task_try_count': 0,
                        'total_workers': 0,
                        'active_workers': 0,
                        'max_concurrent_workers': 0,
                    }
                if status == 'END':
                    manager_info['time_end'] = timestamp
                    manager_info['lifetime(s)'] = round(manager_info['time_end'] - manager_info['time_start'], 2)
        pbar.close()

    return task_info, task_try_count, library_info, worker_info, manager_info, file_info


def parse_debug(debug, worker_info, task_info, task_try_count, manager_info, file_info):

    total_lines = 0
    with open(debug, 'r') as file:
        for line in file:
            total_lines += 1

    putting_file = False
    worker_address_hash_map = {}

    with open(debug, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing debug")
        for line in file:
            pbar.update(1)
            parts = line.strip().split(" ")

            if "worker-id" in parts:
                worker_id_id = parts.index("worker-id")
                worker_hash = parts[worker_id_id + 1]
                worker_machine_name = parts[worker_id_id - 3]
                worker_ip, worker_port = parts[worker_id_id - 2][1:-2].split(':')
                worker_address_hash_map[(worker_ip, worker_port)] = worker_hash
                if worker_hash in worker_info:
                    worker_info[worker_hash]['worker_machine_name'] = worker_machine_name
                    worker_info[worker_hash]['worker_ip'] = worker_ip
                    worker_info[worker_hash]['worker_port'] = worker_port

            if "put" in parts:
                putting_file = True
                continue
            if putting_file:
                putting_file = False
                if "file" not in parts:
                    continue
                file_id = parts.index("file")
                filename = parts[file_id + 1]
                size = int(parts[file_id + 2]) / 2**20
                start_time = float(parts[file_id + 4])
                if start_time < manager_info['time_start']:
                    if abs(start_time - manager_info['time_start']) < 1:
                        start_time = manager_info['time_start']
                    else:
                        print(f"Warning: put start time {start_time} is before manager start time {manager_info['time_start']}")
                worker_ip, worker_port = parts[file_id - 1][1:-2].split(':')
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                if filename not in worker_info[worker_hash]['disk_update']:
                    worker_info[worker_hash]['disk_update'][filename] = {
                        'size(MB)': size,
                        'when_stage_in': [start_time],
                        'when_stage_out': [],
                    }
                    
                else:
                    worker_info[worker_hash]['disk_update'][filename]['when_stage_in'].append(start_time)

                if (size != worker_info[worker_hash]['disk_update'][filename]['size(MB)']):
                    print("Warning: size mismatch for file", filename, "size in debug: ", size, "size in txn: ", worker_info[worker_hash]['disk_update'][filename]['size(MB)'])
                if filename not in file_info:
                    file_info[filename] = {
                        'size(MB)': size,
                        'producers': [],
                        'consumers': [],
                        'worker_holding': [],
                    }

            if "cache-update" in parts:
                # already handled in parse_txn
                continue

            if "infile" in parts or "outfile" in parts:
                file_id = parts.index("infile") if "infile" in parts else parts.index("outfile")
                worker_address = parts[file_id - 1][1:-2]
                worker_ip, worker_port = worker_address.split(':')
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                cached_name = parts[file_id + 1]
                manager_site_name = parts[file_id + 2]

                if manager_site_name in file_info:
                    del file_info[manager_site_name]
                else:
                    # this is normal, as we use the manager_site_name for inputs in the transactions log
                    # and the cached_name for outputs in the debug log
                    pass

                # update disk usage
                if manager_site_name in worker_info[worker_hash]['disk_update']:
                    del worker_info[worker_hash]['disk_update'][manager_site_name]
                    
            if "unlink" in parts:
                unlink_id = parts.index("unlink")
                filename = parts[unlink_id + 1]
                worker_ip, worker_port = parts[unlink_id - 1][1:-2].split(':')
                datestring = parts[0] + " " + parts[1]
                timestamp = datestring_to_timestamp(datestring)
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                worker_id = worker_info[worker_hash]['worker_id']
                try:
                    worker_info[worker_hash]['disk_update'][filename]['when_stage_out'].append(timestamp)
                    if len(worker_info[worker_hash]['disk_update'][filename]['when_stage_out']) > len(worker_info[worker_hash]['disk_update'][filename]['when_stage_in']):
                        print(f"Warning: file {filename} stage out more than stage in for worker {worker_hash}")
                    if filename not in file_info:
                        print(f"Warning: file {filename} not found in file_info")
                        file_info[filename] = {
                            'size(MB)': 0,
                            'producers': [],
                            'consumers': [],
                            'worker_holding': [],
                        }
                    file_info[filename]['worker_holding'].append((worker_id, worker_info[worker_hash]['disk_update'][filename]['when_stage_in'][-1], timestamp))
                except KeyError:
                    pass
                    # print("Warning: file", filename, f"not found in disk_update for worker {worker_ip}:{worker_port} {worker_hash}")    

            if "Submitted" in parts and "recovery" in parts and "task" in parts:
                task_id = int(parts[parts.index("task") + 1])
                try_count = task_try_count[task_id]
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['is_recovery_task'] = True
                    task_info[(task_id, try_id)]['category'] = "recovery_task"
        pbar.close()
    return worker_info


def parse_taskgraph(taskgraph, task_info, task_try_count, file_info):
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
                task_info[(task_id, try_id)]['output_files'].append(filename)
                if filename not in file_info:
                    print(f"Warning: file {filename} not found in file_info")
                file_info[filename]['producers'].append(task_id)
            # task consumes an input file
            elif right.startswith('task'):
                filename = left.split('-', 1)[1]
                task_id = int(right.split('-')[1])
                try_id = task_try_count[task_id]
                task_info[(task_id, try_id)]['input_files'].append(filename)
                if filename not in file_info:
                    file_info[filename] = {
                        'size(MB)': 0,
                        'producers': [],
                        'consumers': [],
                        'worker_holding': [],
                    }
                file_info[filename]['consumers'].append(task_id)
        pbar.close()

    return task_info
