import pandas as pd
import os
import tqdm
import ast
import graphviz
import argparse
from collections import deque
from multiprocessing import Pool, cpu_count, set_start_method



def safe_literal_eval(val):
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []

class EdgeNode:
    def __init__(self, tail, head, weight=None, tail_link=None, head_link=None):
        self.tail = tail
        self.head = head
        self.weight = weight
        self.tail_link = tail_link
        self.head_link = head_link

class VertexNode:
    def __init__(self, task_id):
        self.task_id = task_id
        self.first_in = None
        self.first_out = None
        self.task_life_time = 0

class OrthogonalListGraph:
    def __init__(self):
        self.vertices = {}
        self.edges = {}
        self.components = []

    def get_num_of_components(self):
        return len(self.components)

    def add_vertex(self, task_id, task_life_time=0):
        if task_id not in self.vertices:
            self.vertices[task_id] = VertexNode(task_id)
            self.vertices[task_id].task_life_time = task_life_time
        else:
            print(f"Warning: Task {task_id} already exists.")

    def add_edge(self, tail, head, weight):
        if (tail, head) in self.edges:
            return 
        new_edge = EdgeNode(tail, head, weight)
        
        if tail in self.vertices:
            new_edge.tail_link = self.vertices[tail].first_out
            self.vertices[tail].first_out = new_edge
        if head in self.vertices:
            new_edge.head_link = self.vertices[head].first_in
            self.vertices[head].first_in = new_edge

        self.edges[(tail, head)] = new_edge

    def display(self):
        for v in self.vertices.values():
            print(f"Vertex {v.task_id}:")
            out_edge = v.first_out
            while out_edge:
                print(f"  Out to {out_edge.head}")
                out_edge = out_edge.tail_link
            in_edge = v.first_in
            while in_edge:
                print(f"  In from {in_edge.tail}")
                in_edge = in_edge.head_link

    def update_components(self):
        visited = set()
        self.components = []

        def dfs(v, component):
            visited.add(v)
            component.append(v)
            edge = self.vertices[v].first_out
            while edge:
                if edge.head not in visited:
                    dfs(edge.head, component)
                edge = edge.tail_link
            edge = self.vertices[v].first_in
            while edge:
                if edge.tail not in visited:
                    dfs(edge.tail, component)
                edge = edge.head_link

        for vertex in self.vertices:
            if vertex not in visited:
                component = []
                dfs(vertex, component)
                self.components.append(component)

    def find_critical_path_in_component(self, component):
        in_degree = {v: 0 for v in component}
        longest_path = {v: 0 for v in component}
        predecessor = {v: None for v in component}

        # Calculate in-degrees
        for v in component:
            vertex = self.vertices[v]
            edge = vertex.first_out
            while edge:
                if edge.head in component:
                    in_degree[edge.head] += 1
                edge = edge.tail_link

        # Initialize queue with vertices having zero in-degree
        queue = deque([v for v in component if in_degree[v] == 0])
        
        # Topological order and longest path calculation
        topo_order = []
        while queue:
            v = queue.popleft()
            topo_order.append(v)
            edge = self.vertices[v].first_out
            while edge:
                if edge.head in component:
                    if longest_path[edge.head] < longest_path[v] + self.vertices[edge.head].task_life_time + edge.weight:
                        longest_path[edge.head] = longest_path[v] + self.vertices[edge.head].task_life_time + edge.weight
                        predecessor[edge.head] = v
                    in_degree[edge.head] -= 1
                    if in_degree[edge.head] == 0:
                        queue.append(edge.head)
                edge = edge.tail_link

        # Find the maximum length and construct the critical path
        max_len = max(longest_path.values())
        critical_path = []
        for v in longest_path:
            if longest_path[v] == max_len:
                cur = v
                while cur is not None:
                    critical_path.append(cur)
                    cur = predecessor[cur]
                break

        critical_path.reverse()
        return critical_path
    
    def plot_component(self, component, view=False, save_to=None):
        if not save_to:
            print("Error: save_to is not provided.")
            return
        dot = graphviz.Digraph()
        
        # create nodes, each node is a task
        for task_id in component:
            dot.node(str(task_id), str(task_id))
            this_task = task_done_df[task_done_df['task_id'] == task_id].iloc[0]
            # highlight recovery tasks
            if this_task['is_recovery_task']:
                dot.node(str(task_id), str(task_id), style='filled', color='#ea67a9', shape='ellipse')
            else:
                dot.node(str(task_id), str(task_id), shape='ellipse')

            if args.no_files:
                # plot edges from this task to its successors
                edge = self.vertices[task_id].first_out
                while edge:
                    label = f"{edge.weight}s" if not args.no_weight else None
                    dot.edge(str(task_id), str(edge.head), label=label)
                    edge = edge.tail_link
            else:
                # plot edges from input files to this task
                for input_file in task_done_df[task_done_df['task_id'] == task_id]['input_files'].values[0]:
                    file = general_statistics_file_df[general_statistics_file_df['filename'] == input_file].iloc[0]
                    actual_producer_task_id = 0
                    actual_producer_task = None
                    for producer_task_id in file['producers']:
                        # the producers are already sorted by time
                        producer_task = task_done_df[task_done_df['task_id'] == producer_task_id].iloc[0]
                        if float(producer_task[task_finish_timestamp]) <= float(this_task[task_start_timestamp]):
                            actual_producer_task_id = producer_task_id
                            actual_producer_task = producer_task
                    if actual_producer_task is None:
                        print(f"Warning: Task {task_id} has no producer task for input file {input_file}.")
                    time_period = round(float(this_task[task_start_timestamp]) - float(actual_producer_task[task_finish_timestamp]), 4)
                    label = f"{time_period}s" if not args.no_weight else None
                    if time_period < 0:
                        # it means that this input file is lost after this task is done and it is used as another task's input file                        
                        print(f"Warning: Task {task_id} is started before its producer task {actual_producer_task_id} is finished.")
                        continue
                    dot.node(input_file, input_file, shape='box')
                    if this_task['is_recovery_task'] or actual_producer_task['is_recovery_task']:
                        dot.edge(input_file, str(task_id), color='#ea67a9', style='dashed', label=label)
                    else:
                        dot.edge(input_file, str(task_id), label=label)
                # plot edges from this task to output files
                for output_file in task_done_df[task_done_df['task_id'] == task_id]['output_files'].values[0]:
                    time_period = round(float(this_task[task_finish_timestamp]) - float(this_task[task_start_timestamp]), 4)
                    label = f"{time_period}s" if not args.no_weight else None
                    dot.node(output_file, output_file, shape='box')
                    if this_task['is_recovery_task']:
                        dot.edge(str(task_id), output_file, label=label, color='#ea67a9', style='dashed')
                    else:
                        dot.edge(str(task_id), output_file, label=label)

        dot.attr(rankdir='TB')
        dot.render(save_to, format='svg', view=view)


def process_component(args):
    graph, component, graph_id = args
    task_done_df.loc[task_done_df['task_id'].isin(component), 'graph_id'] = graph_id

    graph.plot_component(component, save_to=os.path.join(dirname, f"subgraph_{graph_id}"), view=False)
    root = component[0]
    graph_info = {
        'graph_id': graph_id,
        'num_tasks': len(component),
        'num_critical_tasks': 0,
        'critical_tasks': 0,
        'time_critical_nodes': [],
        'time_critical_edges': [],
        'time_critical_path': 0,
        'tasks': component,
    }
    graph_info['critical_tasks'] = graph.find_critical_path_in_component(component)
    graph_info['num_critical_tasks'] = len(graph_info['critical_tasks'])
    for i, task_id in enumerate(graph_info['critical_tasks']):
        graph_info['time_critical_nodes'].append(graph.vertices[task_id].task_life_time)
        if i < len(graph_info['critical_tasks']) - 1:
            graph_info['time_critical_edges'].append(graph.edges[(task_id, graph_info['critical_tasks'][i+1])].weight)

    graph_info['time_critical_path'] = sum(graph_info['time_critical_nodes']) + sum(graph_info['time_critical_edges'])

    return root, graph_info


def generate_graph():
    print("Generating graph...")
    graph = OrthogonalListGraph()
    input_to_tasks = {}

    task_done_list = task_done_df.to_dict('records')

    for task in task_done_list:
        execution_time = round(float(task[task_finish_timestamp]) - float(task[task_start_timestamp]), 4)
        graph.add_vertex(task['task_id'], execution_time)
        for file in task['input_files']:
            input_to_tasks.setdefault(file, []).append(task['task_id'])

    for task in task_done_list:
        for file in task['output_files']:
            if file in input_to_tasks:
                tail_task = task
                for target_task_id in input_to_tasks[file]:
                    head_task = task_done_df[task_done_df['task_id'] == target_task_id].iloc[0]
                    weight = round(float(head_task[task_start_timestamp]) - float(tail_task[task_finish_timestamp]), 4)
                    graph.add_edge(task['task_id'], target_task_id, weight=weight)

    graph.update_components()

    return graph

def generate_sub_graphs(graph):
    print(f"Processing components with {cpu_count()} cores...")
    graph_info = {}

    pbar = tqdm.tqdm(total=len(graph.components))
    with Pool(cpu_count()) as pool:
        results = pool.imap_unordered(process_component, [(graph, component, i + 1) for i, component in enumerate(graph.components)])
        for root, info in results:
            graph_info[root] = info
            pbar.update(1)    
    pbar.close()

    graph_info_df = pd.DataFrame.from_dict(graph_info, orient='index')
    graph_info_df.to_csv(os.path.join(dirname, 'graph_info.csv'), index=False)
    task_done_df.to_csv(os.path.join(dirname, 'task_done.csv'), index=False)


if __name__ == '__main__':
    set_start_method('fork')

    parser = argparse.ArgumentParser()
    parser.add_argument('log_dir', type=str, help='the target log directory')
    parser.add_argument('--no-files', action='store_true')
    parser.add_argument('--no-weight', action='store_true')
    args = parser.parse_args()

    task_start_timestamp = 'time_worker_start'
    task_finish_timestamp = 'time_worker_end'

    logs_dir = os.path.join(os.getcwd(), 'logs')
    dirname = os.path.join(args.log_dir, 'vine-logs')

    task_done_df = pd.read_csv(os.path.join(dirname, 'task_done.csv'))
    task_done_df['input_files'] = task_done_df['input_files'].apply(safe_literal_eval)
    task_done_df['output_files'] = task_done_df['output_files'].apply(safe_literal_eval)

    general_statistics_file_df = pd.read_csv(os.path.join(dirname, 'general_statistics_file.csv'))
    general_statistics_file_df['producers'] = general_statistics_file_df['producers'].apply(safe_literal_eval)
    general_statistics_file_df['consumers'] = general_statistics_file_df['consumers'].apply(safe_literal_eval)
    
    graph = generate_graph()
    generate_sub_graphs(graph)