import argparse
import os
import copy
import json
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
import json
from bitarray import bitarray # type: ignore
from tqdm import tqdm
import re
from datetime import datetime, timezone
import pytz
import numpy as np


# initialize the global variables
task_info, task_try_count, library_info, worker_info, manager_info, file_info = {}, {}, {}, {}, {}, {}

############################################################################################################
# Helper functions
def datestring_to_timestamp(datestring):
    eastern = pytz.timezone('US/Eastern')
    date_obj = datetime.strptime(datestring, "%Y/%m/%d %H:%M:%S.%f")
    localized_date = eastern.localize(date_obj)
    unix_timestamp = localized_date.timestamp()
    return unix_timestamp

def timestamp_to_datestring(unix_timestamp):
    eastern = pytz.timezone('US/Eastern')
    date_utc = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
    date_eastern = date_utc.astimezone(eastern)
    datestring = date_eastern.strftime("%Y/%m/%d %H:%M:%S.%f")
    return datestring

def get_worker_ip_port_by_hash(worker_address_hash_map, worker_hash):
    workers_by_ip_port = []
    for k, v in worker_address_hash_map.items():
        if v == worker_hash:
            workers_by_ip_port.append(k[0] + ":" + k[1])
    return workers_by_ip_port
############################################################################################################

############################################################################################################
# Parse functions
def parse_txn():
    worker_coremap = {}

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
                        'execution_time': None,           # spans from time_worker_start to time_worker_end

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
                        'size_input_files(MB)': 0,
                        'size_output_files(MB)': 0,
                        'critical_parent': None,                # task_id of the most recent ready parent
                        'critical_input_file': None,            # input file that took the shortest time to use
                        'critical_input_file_wait_time': None,  # wait time from when the input file was ready to when it was used
                        'is_recovery_task': False,

                        'graph_id': -1,                       # will be set in dag part

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
                        task['execution_time'] = task['time_worker_end'] - task['time_worker_start']
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
                        worker_info[worker_hash]['tasks_completed'].add(task_id)
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
                            'tasks_completed': set(),
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
                        pass
                    elif status == 'CACHE_UPDATE':
                        # will handle in debug parsing
                        pass
    

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
                    manager_info['time_start'] = timestamp
                    manager_info['time_end'] = None
                    manager_info['lifetime(s)'] = None
                    manager_info['time_start_human'] = None
                    manager_info['time_end_human'] = None
                    manager_info['tasks_submitted'] = 0
                    manager_info['tasks_done'] = 0
                    manager_info['tasks_failed_on_manager'] = 0
                    manager_info['tasks_failed_on_worker'] = 0
                    manager_info['max_task_try_count'] = 0
                    manager_info['total_workers'] = 0
                    manager_info['max_concurrent_workers'] = 0
                    manager_info['failed'] = 0

                if status == 'END':
                    manager_info['time_end'] = timestamp
                    manager_info['lifetime(s)'] = round(manager_info['time_end'] - manager_info['time_start'], 2)
        pbar.close()

    if manager_info['time_end'] is None:
        # if the manager did not end, set the end time to the last txn timestamp
        manager_info['time_end'] = timestamp
        manager_info['lifetime(s)'] = round(manager_info['time_end'] - manager_info['time_start'], 2)
        manager_info['failed'] = True

def parse_debug():
    global worker_info
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

            if "info" in parts and "worker-id" in parts:
                worker_id_id = parts.index("worker-id")
                worker_hash = parts[worker_id_id + 1]
                worker_machine_name = parts[worker_id_id - 3]
                worker_ip, worker_port = parts[worker_id_id - 2][1:-2].split(':')
                worker_address_hash_map[(worker_ip, worker_port)] = worker_hash
                if worker_hash in worker_info:
                    worker_info[worker_hash]['worker_machine_name'] = worker_machine_name
                    worker_info[worker_hash]['worker_ip'] = worker_ip
                    worker_info[worker_hash]['worker_port'] = worker_port

            elif "put" in parts:
                putting_file = True
                continue
            elif putting_file:
                if not ("file" in parts and parts[parts.index("file") - 1].endswith(':')):
                    continue
                putting_file = False
                file_id = parts.index("file")
                filename = parts[file_id + 1]
                size_in_mb = int(parts[file_id + 2]) / 2**20
                start_time = float(parts[file_id + 4])
                if start_time < manager_info['time_start']:
                    if abs(start_time - manager_info['time_start']) < 1:
                        start_time = manager_info['time_start']
                    elif start_time == 0:
                        # we have a special file with start time 0
                        start_time = manager_info['time_start']
                    else:
                        print(f"Warning: put start time {start_time} of file {filename} on worker {worker_hash} is before manager start time {manager_info['time_start']}")
                worker_ip, worker_port = parts[file_id - 1][1:-2].split(':')
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                # this is the first time the file is cached on this worker
                # assume the start time is the same as the stage in time if put by the manager
                if filename not in worker_info[worker_hash]['disk_update']:
                    worker_info[worker_hash]['disk_update'][filename] = {
                        'size(MB)': size_in_mb,
                        'when_start_stage_in': [start_time],
                        'when_stage_in': [start_time],
                        'when_stage_out': [],
                    }
                else:
                    worker_info[worker_hash]['disk_update'][filename]['when_start_stage_in'].append(start_time)
                    worker_info[worker_hash]['disk_update'][filename]['when_stage_in'].append(start_time)

            elif "puturl" in parts or "puturl_now" in parts:
                puturl_id = parts.index("puturl") if "puturl" in parts else parts.index("puturl_now")
                url_source = parts[puturl_id + 1]
                worker_ip, worker_port = parts[puturl_id - 1][1:-2].split(':')
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                filename = parts[puturl_id + 2]
                cache_level = parts[puturl_id + 3]
                size_in_mb = int(parts[puturl_id + 4]) / 2**20
                datestring = parts[0] + " " + parts[1]
                timestamp = datestring_to_timestamp(datestring)

                # update disk usage
                if filename not in worker_info[worker_hash]['disk_update']:
                    # this is the first time the file is cached on this worker
                    worker_info[worker_hash]['disk_update'][filename] = {
                        'size(MB)': size_in_mb,
                        'when_start_stage_in': [timestamp],
                        'when_stage_in': [],
                        'when_stage_out': [],
                    }
                else:
                    # already cached previously, start a new cache here
                    worker_info[worker_hash]['disk_update'][filename]['when_start_stage_in'].append(timestamp)

            elif "cache-update" in parts:
                # cache-update cachename, &type, &cache_level, &size, &mtime, &transfer_time, &start_time, id
                # type: VINE_FILE=1, VINE_URL=2, VINE_TEMP=3, VINE_BUFFER=4, VINE_MINI_TASK=5
                # cache_level: 
                #    VINE_CACHE_LEVEL_TASK = 0,     /**< Do not cache file at worker. (default) */
                #    VINE_CACHE_LEVEL_WORKFLOW = 1, /**< File remains in cache of worker until workflow ends. */
                #    VINE_CACHE_LEVEL_WORKER = 2,   /**< File remains in cache of worker until worker terminates. */
                #    VINE_CACHE_LEVEL_FOREVER = 3   /**< File remains at execution site when worker terminates. (use with caution) */

                cache_update_id = parts.index("cache-update")
                filename = parts[cache_update_id + 1]
                file_type = parts[cache_update_id + 2]
                cache_level = parts[cache_update_id + 3]

                size_in_mb = int(parts[cache_update_id + 4]) / 2**20
                wall_time = float(parts[cache_update_id + 6]) / 1e6
                start_time = float(parts[cache_update_id + 7]) / 1e6

                worker_ip, worker_port = parts[cache_update_id - 1][1:-2].split(':')
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]

                # start time should be after the manager start time
                if start_time < manager_info['time_start']:
                    # consider xxx.04224 and xxx.0 as the same time
                    if abs(start_time - manager_info['time_start']) < 1:
                        start_time = manager_info['time_start']
                    else:
                        print(f"Warning: cache-update start time {start_time} is before manager start time {manager_info['time_start']}")

                # update disk usage
                if filename not in worker_info[worker_hash]['disk_update']:
                    # this is the first time the file is cached on this worker
                    worker_info[worker_hash]['disk_update'][filename] = {
                        'size(MB)': size_in_mb,
                        'when_start_stage_in': [start_time],
                        'when_stage_in': [start_time + wall_time],
                        'when_stage_out': [],
                    }
                else:
                    # the start time has been indicated in the puturl message, so we don't need to update it here
                    worker_info[worker_hash]['disk_update'][filename]['when_stage_in'].append(start_time + wall_time)

            elif ("infile" in parts or "outfile" in parts) and "needs" not in parts:
                file_id = parts.index("infile") if "infile" in parts else parts.index("outfile")
                worker_address = parts[file_id - 1][1:-2]
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                cached_name = parts[file_id + 1]
                manager_site_name = parts[file_id + 2]

                # update disk usage
                if manager_site_name in worker_info[worker_hash]['disk_update']:
                    del worker_info[worker_hash]['disk_update'][manager_site_name]
            
            elif "unlink" in parts:
                unlink_id = parts.index("unlink")
                filename = parts[unlink_id + 1]
                worker_ip, worker_port = parts[unlink_id - 1][1:-2].split(':')
                datestring = parts[0] + " " + parts[1]
                timestamp = datestring_to_timestamp(datestring)
                worker_hash = worker_address_hash_map[(worker_ip, worker_port)]
                worker_id = worker_info[worker_hash]['worker_id']
                
                if filename not in worker_info[worker_hash]['disk_update']:
                    print(f"Warning: file {filename} not in worker {worker_hash}")
                    print(f"workers: {get_worker_ip_port_by_hash(worker_address_hash_map, worker_hash)}")
                worker_when_start_stage_in = worker_info[worker_hash]['disk_update'][filename]['when_start_stage_in']
                worker_when_stage_in = worker_info[worker_hash]['disk_update'][filename]['when_stage_in']
                worker_when_stage_out = worker_info[worker_hash]['disk_update'][filename]['when_stage_out']
                worker_when_stage_out.append(timestamp)

                # in some case when using puturl or puturl_now, we may fail to receive the cache-update message, use the start time as the stage in time
                if len(worker_when_start_stage_in) != len(worker_when_stage_in):
                    for i in range(len(worker_when_start_stage_in) - len(worker_when_stage_in)):
                        worker_when_stage_in.append(worker_when_start_stage_in[len(worker_when_start_stage_in) - i - 1])
                
            elif "Submitted" in parts and "recovery" in parts and "task" in parts:
                task_id = int(parts[parts.index("task") + 1])
                try_count = task_try_count[task_id]
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['is_recovery_task'] = True
                    task_info[(task_id, try_id)]['category'] = "recovery_task"
        pbar.close()

    for worker_hash, worker in worker_info.items():
        for filename, worker_disk_update in worker['disk_update'].items():
            len_stage_in = len(worker_disk_update['when_stage_in'])
            len_stage_out = len(worker_disk_update['when_stage_out'])
            if filename not in file_info:
                file_info[filename] = {
                    'size(MB)': round(worker_disk_update['size(MB)'], 6),
                    'producers': [],
                    'consumers': [],
                    'worker_holding': [],
                }
            for i in range(len_stage_out):
                worker_holding = {
                    'worker_hash': worker_hash,
                    'time_stage_in': worker_disk_update['when_stage_in'][i],
                    'time_stage_out': worker_disk_update['when_stage_out'][i],
                }
                file_info[filename]['worker_holding'].append(worker_holding)
            # in case some files are not staged out, consider the manager end time as the stage out time
            if len_stage_out < len_stage_in:
                print(f"Warning: file {filename} stage out less than stage in for worker {worker_hash}, stage_in: {len_stage_in}, stage_out: {len_stage_out}")
                for i in range(len_stage_in - len_stage_out):
                    worker_holding = {
                        'worker_hash': worker_hash,
                        'time_stage_in': worker_disk_update['when_stage_in'][len_stage_in - i - 1],
                        'time_stage_out': manager_info['time_end'],
                    }

    # filter out the workers that are not active
    manager_info['total_workers'] = len(worker_info)
    active_workers = set()
    for task in task_info.values():
        active_workers.add(task['worker_committed'])
    worker_info = {worker_hash: worker for worker_hash, worker in worker_info.items() if worker_hash in active_workers}
    worker_info = {k: v for k, v in sorted(worker_info.items(), key=lambda item: item[1]['time_connected'])}
    manager_info['active_workers'] = len(worker_info)

    # Add worker_id to worker_info and update the relevant segments in task_info and library_info
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
    
    with open(os.path.join(dirname, 'worker_info.json'), 'w') as f:
        json.dump(worker_info, f, indent=4)


def parse_taskgraph():
    total_lines = 0
    with open(taskgraph, 'r') as file:
        for line in file:
            total_lines += 1

    with open(taskgraph, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing taskgraph")
        line_id = 0
        for line in file:
            line_id += 1
            pbar.update(1)
            if '->' not in line:
                continue
            try:
                left, right = line.split(' -> ')
                left = left.strip().strip('"')
                right = right.strip()[:-1].strip('"')
            except ValueError:
                print(f"Warning: Unexpected format: {line}")
                continue

            # task produces an output file
            try:
                if left.startswith('task'):
                    filename = right.split('-', 1)[1]
                    task_id = int(left.split('-')[1])
                    try_id = task_try_count[task_id]
                    task_info[(task_id, try_id)]['output_files'].append(filename)
                    if filename not in file_info:
                        # if we approach the final line, the filename may be invalid because the manager hasn't finished
                        if line_id == total_lines:
                            print(f"Warning: file {filename} not found in file_info, this may be due to the manager not finishing")
                            break
                        else:
                            raise ValueError(f"file {filename} not found in file_info")
                    file_info[filename]['producers'].append(task_id)
                # task consumes an input file
                elif right.startswith('task'):
                    filename = left.split('-', 1)[1]
                    task_id = int(right.split('-')[1])
                    try_id = task_try_count[task_id]
                    task_info[(task_id, try_id)]['input_files'].append(filename)
                    if filename not in file_info:
                        raise ValueError(f"file {filename} not found in file_info")
                    file_info[filename]['consumers'].append(task_id)
            except IndexError:
                    print(f"Warning: Unexpected format: {line}")
                    continue
        pbar.close()

        # we only consider files produced by another task as input files
        for task in task_info.values():
            cleaned_input_files = []
            for input_file in task['input_files']:
                if file_info[input_file]['producers']:
                    cleaned_input_files.append(input_file)
            task['input_files'] = cleaned_input_files
    
    # calculate the size of input and output files
    print(f"Generating file_info.csv...")
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


def parse_daskvine_log():
    # check if the daskvine exists
    try:
        with open(daskvine_log, 'r') as file:
            pass
    except FileNotFoundError:
        return
    
    total_lines = 0
    with open(daskvine_log, 'r') as file:
        for line in file:
            total_lines += 1

    with open(daskvine_log, 'r') as file:
        pbar = tqdm(total=total_lines, desc="parsing daskvine log")
        for line in file:
            pbar.update(1)
            parts = line.strip().split(" ")

            event, timestamp, task_id = parts[0], int(parts[1]), int(parts[2])
            try_count = task_try_count[task_id]
            if event == "submitted":
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['when_submitted_by_daskvine'] = timestamp
            if event == 'received':
                for try_id in range(1, try_count + 1):
                    task_info[(task_id, try_id)]['when_received_by_daskvine'] = timestamp

        pbar.close()
############################################################################################################


def generate_worker_summary(task_df, worker_disk_usage_df):
    print(f"Generating worker_summary.csv...")

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
            'num_tasks_completed': 0,
            'avg_task_runtime(s)': 0,
            'peak_disk_usage(MB)': 0,
            'peak_disk_usage(%)': 0,
        }
        # calculate the number of tasks done by this worker
        row['num_tasks_completed'] = len(len(worker_info[worker_hash]['tasks_completed']))
        # check if this worker has any disk updates
        if not worker_disk_usage_df.empty and worker_disk_usage_df['worker_hash'].isin([worker_hash]).any():
            row['peak_disk_usage(MB)'] = worker_disk_usage_df[worker_disk_usage_df['worker_hash'] == worker_hash]['disk_usage(MB)'].max()
            row['peak_disk_usage(%)'] = worker_disk_usage_df[worker_disk_usage_df['worker_hash'] == worker_hash]['disk_usage(%)'].max()
        # the worker may not complete any tasks
        if row['num_tasks_completed'] > 0:
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

def generate_general_statistics(task_df, worker_summary_df):
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
    worker_connection_events_df = pd.DataFrame(worker_connection_events, columns=['time', 'parallel_workers', 'event', 'worker_id'])
    worker_connection_events_df.to_csv(os.path.join(dirname, 'worker_connections.csv'), index=False)

    manager_info['max_concurrent_workers'] = max([x[1] for x in worker_connection_events])
    row_task_total = general_statistics_task_df[general_statistics_task_df['category'] == 'TOTAL']
    manager_info['tasks_submitted'] = row_task_total['submitted'].iloc[0]
    manager_info['time_start_human'] = timestamp_to_datestring(manager_info['time_start'])
    manager_info['time_end_human'] = timestamp_to_datestring(manager_info['time_end'])
    # the max try_id in task_df
    manager_info['max_task_try_count'] = task_df['try_id'].max()
    manager_info_df = pd.DataFrame([manager_info])
    manager_info_df.to_csv(os.path.join(dirname, 'general_statistics_manager.csv'), index=False)
    #####################################################

def generate_library_summary():
    library_df = pd.DataFrame.from_dict(library_info, orient='index')
    library_df.to_csv(os.path.join(dirname, 'library_summary.csv'), index=False)


def handle_task_info():
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
        # if the when_next_ready is na, that means the manager exited before the task was ready, set it to the worker end time
        if pd.isna(task['when_next_ready']):
            worker = worker_info[task['worker_committed']]
            for i in range(len(worker['time_connected'])):
                if len(worker['time_disconnected']) != len(worker['time_connected']):
                    # worker is still connected
                    worker['time_disconnected'].append(manager_info['time_end'])
                else:
                    if worker['time_connected'][i] < task['when_running'] and worker['time_disconnected'][i] > task['when_running']:
                        task['when_next_ready'] = worker['time_disconnected'][i]
        
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
    
    # the concurrent tasks throughout the manager's lifetime
    # skip if when_running is na
    task_running_df = pd.DataFrame({
        'time': task_df['when_running'].dropna(),
        'task_id': task_df['task_id'],
        'worker_id': task_df['worker_id'],
        'category': task_df['category'],
        'type': 1
    })
    # skip if when_waiting_retrieval is na, use when_next_ready instead
    task_waiting_retrieval_df = pd.DataFrame({
        'time': task_df.apply(lambda row: row['when_waiting_retrieval'] if pd.notna(row['when_waiting_retrieval']) else row['when_next_ready'], axis=1).dropna(),
        'task_id': task_df['task_id'],
        'worker_id': task_df['worker_id'],
        'category': task_df['category'],
        'type': -1
    })
    events_df = pd.concat([task_running_df, task_waiting_retrieval_df]).sort_values('time')

    events_df = events_df.sort_values('time')
    events_df['concurrent_tasks'] = events_df['type'].cumsum()
    events_df.to_csv(os.path.join(dirname, 'task_concurrency.csv'), index=False)

    task_df[is_done].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_done.csv'), index=False)
    task_df[is_failed_manager].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_failed_on_manager.csv'), index=False)
    task_df[is_failed_worker].apply(handle_each_task, axis=1).to_csv(os.path.join(dirname, 'task_failed_on_worker.csv'), index=False)

    return task_df

def generate_worker_disk_usage():
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

def handle_file_info():
    print(f"Generating file_info.csv...")
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

def generate_data():

    parse_txn()

    if not args.execution_details_only:
        parse_debug()
        parse_taskgraph()

    parse_daskvine_log()
    task_df = handle_task_info()
    worker_disk_usage_df  = generate_worker_disk_usage()
    worker_summary_df = generate_worker_summary(task_df, worker_disk_usage_df)
    
    generate_general_statistics(task_df, worker_summary_df)

    # for function calls
    generate_library_summary()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('log_dir', type=str, help='the target log directory')
    parser.add_argument('--execution-details-only', action='store_true', help='Only generate data for task execution details')
    args = parser.parse_args()

    dirname = os.path.join(args.log_dir, 'vine-logs')
    txn = os.path.join(dirname, 'transactions')
    debug = os.path.join(dirname, 'debug')
    taskgraph = os.path.join(dirname, 'taskgraph')
    daskvine_log = os.path.join(dirname, 'daskvine.log')

    # task_id -> max try_count
    task_try_count = {}

    generate_data()
