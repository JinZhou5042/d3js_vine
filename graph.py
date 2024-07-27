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
        self.subgraphs = []

    def get_num_of_subgraphs(self):
        return len(self.subgraphs)

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

    def update_subgraphs(self):
        visited = set()
        self.subgraphs = []

        def dfs(v, subgraph):
            visited.add(v)
            subgraph.append(v)
            edge = self.vertices[v].first_out
            while edge:
                if edge.head not in visited:
                    dfs(edge.head, subgraph)
                edge = edge.tail_link
            edge = self.vertices[v].first_in
            while edge:
                if edge.tail not in visited:
                    dfs(edge.tail, subgraph)
                edge = edge.head_link

        for vertex in self.vertices:
            if vertex not in visited:
                subgraph = []
                dfs(vertex, subgraph)
                self.subgraphs.append(subgraph)

    def find_critical_path_in_subgraph(self, subgraph):
        in_degree = {v: 0 for v in subgraph}
        longest_path = {v: 0 for v in subgraph}
        predecessor = {v: None for v in subgraph}

        # Calculate in-degrees
        for v in subgraph:
            vertex = self.vertices[v]
            edge = vertex.first_out
            while edge:
                if edge.head in subgraph:
                    in_degree[edge.head] += 1
                edge = edge.tail_link

        # Initialize queue with vertices having zero in-degree
        queue = deque([v for v in subgraph if in_degree[v] == 0])
        
        # Topological order and longest path calculation
        topo_order = []
        while queue:
            v = queue.popleft()
            topo_order.append(v)
            edge = self.vertices[v].first_out
            while edge:
                if edge.head in subgraph:
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
    
    def plot_subgraph(self, subgraph, view=False, save_to=None):
        if not save_to:
            print("Error: save_to is not provided.")
            return
        dot = graphviz.Digraph()
        
        # create nodes, each node is a task
        for task_id in subgraph:
            if args.task_node_label == 'task-id':
                task_node_label = str(task_id)
            elif args.task_node_label == 'category-id':
                task_node_label = str(int(task_info[task_id]['category_id'])).split('.')[0]
            dot.node(str(task_id), task_node_label, shape='ellipse')
            this_task = task_info[task_id]
            # highlight recovery tasks
            if this_task['is_recovery_task']:
                dot.node(str(task_id), task_node_label, style='filled', color='#ea67a9', shape='ellipse')
            else:
                dot.node(str(task_id), task_node_label, shape='ellipse')

            if args.no_files:
                # plot edges from this task to its successors
                edge = self.vertices[task_id].first_out
                while edge:
                    edge_label = f"{edge.weight}s" if not args.no_weight else None
                    dot.edge(str(task_id), str(edge.head), label=edge_label)
                    edge = edge.tail_link
            else:
                # plot edges from input files to this task
                for input_file in task_info[task_id]['input_files']:
                    file = file_info_df[file_info_df['filename'] == input_file].iloc[0]
                    actual_producer_task_id = 0
                    actual_producer_task = None
                    for producer_task_id in file['producers']:
                        # the producers are already sorted by time
                        producer_task = task_info[producer_task_id]
                        if float(producer_task[task_finish_timestamp]) <= float(this_task[task_start_timestamp]):
                            actual_producer_task_id = producer_task_id
                            actual_producer_task = producer_task
                    if actual_producer_task is None:
                        print(f"Warning: Task {task_id} has no producer task for input file {input_file}.")
                    time_period = round(float(this_task[task_start_timestamp]) - float(actual_producer_task[task_finish_timestamp]), 4)
                    edge_label = f"{time_period}s" if not args.no_weight else None
                    if time_period < 0:
                        # it means that this input file is lost after this task is done and it is used as another task's input file                        
                        print(f"Warning: Task {task_id} is started before its producer task {actual_producer_task_id} is finished.")
                        continue
                    dot.node(input_file, input_file, shape='box')
                    if this_task['is_recovery_task'] or actual_producer_task['is_recovery_task']:
                        dot.edge(input_file, str(task_id), color='#ea67a9', style='dashed', label=edge_label)
                    else:
                        dot.edge(input_file, str(task_id), label=edge_label)
                # plot edges from this task to output files
                for output_file in task_info[task_id]['output_files']:
                    time_period = round(float(this_task[task_finish_timestamp]) - float(this_task[task_start_timestamp]), 4)
                    edge_label = f"{time_period}s" if not args.no_weight else None
                    dot.node(output_file, output_file, shape='box')
                    if this_task['is_recovery_task']:
                        dot.edge(str(task_id), output_file, label=edge_label, color='#ea67a9', style='dashed')
                    else:
                        dot.edge(str(task_id), output_file, label=edge_label)

        dot.attr(rankdir='TB')
        if args.save_format == 'svg':
            dot.render(save_to, format='svg', view=view)
        elif args.save_format == 'png':
            dot.render(save_to, format='png', view=view)


def process_subgraph(args):
    graph, subgraph, graph_id = args

    for task_id in subgraph:
        task_info[task_id]['graph_id'] = graph_id

    graph.plot_subgraph(subgraph, save_to=os.path.join(dirname, f"subgraph_{graph_id}"), view=False)
    root = subgraph[0]
    graph_info = {
        'graph_id': graph_id,
        'num_tasks': len(subgraph),
        'num_critical_tasks': 0,
        'time_start': 0,
        'time_end': 0,
        'time_completion': 0,
        'critical_tasks': 0,
        'time_completion': 0,
        'tasks': subgraph,
    }
    graph_info['critical_tasks'] = graph.find_critical_path_in_subgraph(subgraph)
    graph_info['num_critical_tasks'] = len(graph_info['critical_tasks'])
    first_task_id = graph_info['critical_tasks'][0]
    last_task_id = graph_info['critical_tasks'][-1]
    graph_info['time_start'] = task_info[first_task_id]['when_ready']
    graph_info['time_end'] = task_info[last_task_id]['when_done']
    graph_info['time_completion'] = task_info[last_task_id]['when_done'] - task_info[first_task_id]['when_ready']

    return root, graph_info

def generate_subgraphs(graph):
    print(f"Processing subgraphs with {cpu_count()} cores...")

    graph_info = {}
    pbar = tqdm.tqdm(total=len(graph.subgraphs))
    with Pool(cpu_count()) as pool:
        results = pool.imap_unordered(process_subgraph, [(graph, subgraph, i + 1) for i, subgraph in enumerate(graph.subgraphs)])
        for root, info in results:
            graph_info[root] = info
            pbar.update(1)
    pbar.close()

    return graph_info


def generate_graph():
    print("Generating graph...")
    graph = OrthogonalListGraph()
    input_to_tasks = {}

    for task_id, task in task_info.items():
        execution_time = round(float(task[task_finish_timestamp]) - float(task[task_start_timestamp]), 4)
        graph.add_vertex(task_id, execution_time)
        for file in task['input_files']:
            input_to_tasks.setdefault(file, []).append(task_id)

    for task_id, task in task_info.items():
        for file in task['output_files']:
            if file in input_to_tasks:
                tail_task = task
                for target_task_id in input_to_tasks[file]:
                    head_task = task_info[target_task_id]
                    weight = round(float(head_task[task_start_timestamp]) - float(tail_task[task_finish_timestamp]), 4)
                    graph.add_edge(task_id, target_task_id, weight=weight)

    graph.update_subgraphs()

    return graph


if __name__ == '__main__':
    set_start_method('fork')

    parser = argparse.ArgumentParser()
    parser.add_argument('log_dir', type=str, help='the target log directory')
    parser.add_argument('--no-files', action='store_true')
    parser.add_argument('--no-weight', action='store_true')
    parser.add_argument('--task-node-label', type=str, default='task-id')
    parser.add_argument('--save_format', type=str, default='svg')
    args = parser.parse_args()

    task_start_timestamp = 'time_worker_start'
    task_finish_timestamp = 'time_worker_end'

    logs_dir = os.path.join(os.getcwd(), 'logs')
    dirname = os.path.join(args.log_dir, 'vine-logs')

    task_done_df = pd.read_csv(os.path.join(dirname, 'task_done.csv'))
    task_done_df['input_files'] = task_done_df['input_files'].apply(safe_literal_eval)
    task_done_df['output_files'] = task_done_df['output_files'].apply(safe_literal_eval)
    task_info = task_done_df.set_index('task_id', inplace=False).to_dict('index') 

    file_info_df = pd.read_csv(os.path.join(dirname, 'file_info.csv'))
    file_info_df['producers'] = file_info_df['producers'].apply(safe_literal_eval)
    file_info_df['consumers'] = file_info_df['consumers'].apply(safe_literal_eval)
    
    graph = generate_graph()
    graph_info = generate_subgraphs(graph)

    # update graph_id for each task
    for i, subgraph in enumerate(graph.subgraphs):
        for task_id in subgraph:
            task_info[task_id]['graph_id'] = i + 1

    graph_info_df = pd.DataFrame.from_dict(graph_info, orient='index')
    graph_info_df.sort_values(by='graph_id', inplace=True)
    graph_info_df.to_csv(os.path.join(dirname, 'graph_info.csv'), index=False)
    task_done_df = pd.DataFrame.from_dict(task_info, orient='index')

    task_done_df.index.name = 'task_id'
    task_done_df.to_csv(os.path.join(dirname, 'task_done.csv'), index=True)
