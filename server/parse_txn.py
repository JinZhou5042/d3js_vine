import pandas as pd
import json

def parse_txn_between_timestamps(txn, start_time=None, end_time=None):
    log_lines = open(txn, 'r').read().splitlines()

    task_info = []
    worker_info = []
    library_info = []
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
                    'when_ready': timestamp,
                    'when_running': None,
                    'when_waiting_retrieval': None,
                    'when_retrieved': None,
                    'when_done': None,
                    
                    'worker_committed': None,
                    'worker_slot': None,
                    'time_commit_start': None,
                    'time_commit_end': None,
                    'time_input_mgr': None,
                    'size_input_mgr': None,
                    'time_worker_start': None,
                    'time_worker_end': None,
                    'time_output_mgr': None,
                    'size_output_mgr': None,

                    #############################
                    # Not yet implemented
                    'when_input_sent': None,
                    'when_output_received': None,
                    #############################

                    'cores_requested': resources_requested.get("cores", [0, ""])[0],
                    'gpus_requested': resources_requested.get("gpus", [0, ""])[0],
                    'memory_requested(MB)': resources_requested.get("memory", [0, ""])[0],
                    'disk_requested(MB)': resources_requested.get("disk", [0, ""])[0],
                    'retrieved_status': None,
                    'done_status': None,
                    'done_code': None,
                    'category': category,
                }
                task_info.append(task)
            if status == 'RUNNING':
                resources_allocated = json.loads(info.split(' ', 3)[-1])
                for task in task_info:
                    if task['task_id'] == obj_id:
                        task['when_running'] = timestamp
                        task['worker_committed'] = info.split()[0]
                        task['time_commit_start'] = resources_allocated["time_commit_start"][0]
                        task['time_commit_end'] = resources_allocated["time_commit_end"][0]
                        task['time_input_mgr'] = resources_allocated["time_input_mgr"][0]
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

                for task in task_info:
                    if task['task_id'] == obj_id:
                        task['when_retrieved'] = timestamp
                        task['retrieved_status'] = status
                        task['time_worker_start'] = resources_retrieved.get("time_worker_start", [None])[0]
                        task['time_worker_end'] = resources_retrieved.get("time_worker_end", [None])[0]
                        task['time_output_mgr'] = resources_retrieved.get("time_output_mgr", [None])[0]
                        task['size_output_mgr'] = resources_retrieved.get("size_output_mgr", [None])[0]
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
                worker_info.append({
                    'worker_id': obj_id,
                    'time_connected': timestamp,
                    'time_disconnected': None
                })
            if status == 'DISCONNECTION':
                for worker in worker_info:
                    if worker['worker_id'] == obj_id:
                        worker['time_disconnected'] = timestamp
                        break
        if category == 'LIBRARY':
            if status == 'STARTED':
                library_info.append({
                    'library_id': obj_id,
                    'time_start': timestamp,
                    'time_end': None,
                    'worker': info
                })
        if category == 'MANAGER':
            if status == 'START':
                manager_info['time_start'] = timestamp
            if status == 'END':
                manager_info['time_end'] = timestamp

    # The stop time for the manager is not recorded in the log, so we set it to the end time of the library task
    for library in library_info:
        if library['time_end'] is None:
            library['time_end'] = manager_info.get('time_end')

    # Convert lists to DataFrames
    task_df = pd.DataFrame(task_info)
    worker_df = pd.DataFrame(worker_info)
    library_df = pd.DataFrame(library_info)

    # Save DataFrames to CSV
    task_df.to_csv('task_info.csv', index=False)
    worker_df.to_csv('worker_info.csv', index=False)
    library_df.to_csv('library_info.csv', index=False)

    return task_df, worker_df, library_df, manager_info

# Example usage
parse_txn_between_timestamps('../logs/2024-05-23T160436/vine-logs/transactions', 0, 1000000000)