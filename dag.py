import pandas as pd
import os
import tqdm
import sys
import graphviz
import matplotlib.pyplot as plt
import numpy as np
from collections import deque, defaultdict

import json

def print_json(json_obj):
    print(json.dumps(json_obj, indent=2))

base_dir = "/Users/jinzhou/Applications/d3js_vine"
logs_dir = os.path.join(base_dir, 'logs')
with os.scandir(logs_dir) as entries:
    for entry in entries:
        if entry.is_dir():
            log_dir = os.path.join('logs', entry.name)
            break

dirname = os.path.join(log_dir, 'vine-logs')
task_dag_csv = os.path.join(dirname, 'task_dag.csv')

task_done_df = pd.read_csv(os.path.join(dirname, 'task_done.csv'))
task_done_df = task_done_df[task_done_df['is_recovery_task'] == False]
task_dag_df = task_done_df[['task_id', 'category', 'when_ready', 'when_done', 'input_files', 'output_files']]
# save
task_dag_df.to_csv(task_dag_csv, index=False)


class EdgeNode:
    def __init__(self, tail, head, weight=None, tail_link=None, head_link=None):
        self.tail = tail
        self.head = head
        self.weight = weight
        self.tail_link = tail_link
        self.head_link = head_link

class VertexNode:
    def __init__(self, vertex_id):
        self.vertex_id = vertex_id
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

    def add_vertex(self, vertex_id, task_life_time=0):
        if vertex_id not in self.vertices:
            self.vertices[vertex_id] = VertexNode(vertex_id)
            self.vertices[vertex_id].task_life_time = task_life_time
        else:
            print(f"Warning: Vertex {vertex_id} already exists.")

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
            print(f"Vertex {v.vertex_id}:")
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
    
    def visualize_components(self):
        if not self.components:
            self.update_components()
        pbar = tqdm.tqdm(total=len(self.components))
        for i, component in enumerate(self.components):
            dot = graphviz.Digraph(comment=f'Task Subgraph {i+1}')
            
            for vertex_id in component:
                dot.node(str(vertex_id), str(vertex_id))
            
            for vertex_id in component:
                vertex = self.vertices[vertex_id]
                edge = vertex.first_out
                while edge:
                    if edge.head in component:
                        dot.edge(str(edge.tail), str(edge.head), label=str(round(edge.weight, 4)))
                    edge = edge.tail_link
            
            dot.attr(rankdir='TB')
            dot.render(f'output/task_subgraph_{i+1}', format='svg', view=False)
            pbar.update(1)
        pbar.close()

import ast

graph = OrthogonalListGraph()

input_to_tasks = {}
for index, row in task_dag_df.iterrows():
    execution_time = round(float(row['when_done']) - float(row['when_ready']), 4)
    graph.add_vertex(row['task_id'], execution_time)
    input_files = ast.literal_eval(row['input_files'])
    for file in input_files:
        if file not in input_to_tasks:
            input_to_tasks[file] = []
        input_to_tasks[file].append(row['task_id'])

for index, row in task_dag_df.iterrows():
    output_files = ast.literal_eval(row['output_files'])
    for file in output_files:
        if file in input_to_tasks:
            for target_task_id in input_to_tasks[file]:
                tail_task = task_dag_df[task_dag_df['task_id'] == row['task_id']].iloc[0]
                head_task = task_dag_df[task_dag_df['task_id'] == target_task_id].iloc[0]
                weight = round(float(head_task['when_ready']) - float(tail_task['when_done']), 4)
                graph.add_edge(row['task_id'], target_task_id, weight=weight)


graph.visualize_components()

for component in graph.components:
    critical_path = graph.find_critical_path_in_component(component)
    total_time = 0
    for i, task_id in enumerate(critical_path):
        total_time += graph.vertices[task_id].task_life_time
        if i < len(critical_path) - 1:
            total_time += graph.edges[(task_id, critical_path[i+1])].weight

    print(f"Critical Path: {critical_path},  total_time: {total_time}")