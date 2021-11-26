import yaml


def bfs(graph, current_vertex, connect='', depth=-1, path='', root_table=None):
    if connect:
        print('{}{}{}'.format('\t' * depth, connect, '──'), end=' ')
    print(current_vertex)
    path = (path + f'/{current_vertex}' if current_vertex != 'root' else '/').replace('//', '/')
    if root_table is None:
        root_table = list()
    root_table.append((current_vertex, path, 'TREE_NODE', graph[current_vertex]['info']))

    if 'contains' not in graph[current_vertex]:
        return

    next_graph = graph[current_vertex]['contains']
    for i, child in enumerate(next_graph):
        symbol = '└' if i == len(next_graph) - 1 else '├'
        bfs(next_graph, current_vertex=child, connect=symbol, depth=depth + 1, path=path, root_table=root_table)


def print_table(table):
    for row in table:
        print('|', ' | '.join([f'`{item}`' for item in row[:-1]] + [row[-1]]), '|')


if __name__ == '__main__':
    path_to_root_nodes_yaml = '../dags/dags/dd_load_dds_root/etc/root_nodes.yaml'
    with open(path_to_root_nodes_yaml) as file:
        try:
            graph_yaml = yaml.safe_load(file)
        except yaml.YAMLError as e:
            print(e)

        root = []
        print('```')
        bfs(graph_yaml, 'root', root_table=root)
        print('```')
        print('''
Description of hierarchy of root entities:

| Name | Path to entity | Entity type | Info |
| --- | --- | --- | --- |''')
        print_table(root)
