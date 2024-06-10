import pandas as pd
import json

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
                        worker_id = task['worker_committed']
                        if worker_id not in worker_slots:
                            worker_slots[worker_id] = []
                        slots = worker_slots[worker_id]
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

    # Convert lists to DataFrames
    task_df = pd.DataFrame(task_info)
    library_df = pd.DataFrame(library_info)

    # Save DataFrames to CSV
    task_df.to_csv('task_info.csv', index=False)
    library_df.to_csv('library_info.csv', index=False)

    with open('worker_info.json', 'w') as f:
        json.dump(worker_info, f, indent=4)


parse_txn('../logs/2024-05-23T160436/vine-logs/transactions')