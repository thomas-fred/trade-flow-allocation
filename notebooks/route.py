import json
import os
import multiprocessing
import time
import sys

import igraph as ig
import geopandas as gpd
import pandas as pd


# dict containing:
# 'value_kusd' -> float
# 'volume_tons' -> float
# 'edge_indicies' -> list[indicies]
FlowResult = dict[str, float | list[int]]

# nested dict with FlowResult leaves
# source node       -> destination country  -> FlowResult dict
# e.g.
# 'thailand_4_123'  -> 'GID_0_GBR'          -> FlowResult dict
RouteResult = dict[str, dict[str, FlowResult]]


def init_worker(graph_filepath: str, od_filepath: str) -> None:
    """
    Create global variables referencing graph and OD to persist through worker lifetime.

    Args:
        graph_filepath: Filepath of pickled igraph.Graph to route over.
        od_filepath: Filepath to table of flows from origin node 'id' to
            destination country 'partner_GID_0', should also contain 'value_kusd'
            and 'volume_tons'.
    """
    print(f"Process {os.getpid()} initialising...")
    global graph
    graph = ig.Graph.Read_Pickle(graph_filepath)
    global od
    od = pd.read_parquet(od_filepath)
    return


def route_from_node(from_node: str) -> RouteResult:
    """
    Route flows from single 'from_node' to destinations across graph. Record value and
    volume flowing across each edge.

    Args:
        from_node: Node ID of source node.

    Returns:
        Mapping from source node, to destination country node, to value of
            flow, volume of flow and list of edge ids of route.
    """
    print(f"Process {os.getpid()} routing {from_node}...")

    from_node_od = od[od.id == from_node]
    destination_nodes = [f"GID_0_{iso_a3}" for iso_a3 in from_node_od.partner_GID_0.unique()]

    routes_edge_list = []
    try:
        routes_edge_list: list[list[int]] = graph.get_shortest_paths(
            f"road_{from_node}",
            destination_nodes,
            weights="cost_USD_t",
            output="epath"
        )
    except ValueError as error:
        if "no such vertex" in str(error):
            print(f"{error}... skipping destination")
            pass

    routes: RouteResult = {}

    if routes_edge_list:
        assert len(routes_edge_list) == len(destination_nodes)
    else:
        return routes

    for i, destination_node in enumerate(destination_nodes):

        # lookup trade value and volume for each pairing of from_node and partner country
        # GID_0_GBR -> GBR
        iso_a3 = destination_node.split("_")[-1]
        route = from_node_od[
            (from_node_od.id == from_node) & (from_node_od.partner_GID_0 == iso_a3)
        ]
        value_kusd, = route.value_kusd
        volume_tons, = route.volume_tons

        routes[from_node] = {
            destination_node: {
                "value_kusd": value_kusd,
                "volume_tons": volume_tons,
                "edge_indicies": routes_edge_list[i]
            }
        }

    return routes


def route_from_all_nodes(od: pd.DataFrame, edges: gpd.GeoDataFrame, n_cpu: int) -> RouteResult:
    """
    Route flows from origins to destinations across graph.

    Args:
        od: Table of flows from origin node 'id' to destination country
            'partner_GID_0', should also contain 'value_kusd' and 'volume_tons'.
        edges: Table of edges to construct graph from. First column should be
            source node id and second destination node id.
        n_cpu: Number of CPUs to use for routing.

    Returns:
        Mapping from source node, to destination country node, to flow in value
            and volume along this route and list of edge indicies constituting
            the route.
    """

    print("Creating graph...")
    # cannot add vertices as edges reference port493_out, port281_in, etc. which are missing from nodes file
    # use_vids=False as edges.from_id and edges_to_id are not integers
    graph = ig.Graph.DataFrame(edges, directed=True, use_vids=False)

    print("Writing graph to disk...")
    graph_filepath = "graph.pickle"
    graph.write_pickle(graph_filepath)

    print("Writing OD to disk...")
    od_filepath = "od.pq"
    od.to_parquet(od_filepath)

    print("Routing...")
    start = time.time()
    from_nodes = od.id.unique()
    routes = []
    args = ((from_node,) for from_node in from_nodes)
    if n_cpu > 1:
        # as each process is created, it will load the graph from disk in init_worker
        # and then persist it in memory between chunks of work
        with multiprocessing.Pool(
            processes=n_cpu,
            initializer=init_worker,
            initargs=(graph_filepath, od_filepath),
        ) as pool:
            routes = pool.starmap(route_from_node, args)
    else:
        for arg in args:
            routes.append(route_from_node(*arg))

    print("\n")
    print(f"Routing completed in {time.time() - start:.2f}s")

    # combine our list of singleton dicts into one dict with multiple keys
    return {k: v for item in routes for (k, v) in item.items()}


if __name__ == "__main__":

    n_cpu = int(sys.argv[1])

    root_dir = ".."

    print("Reading network...")
    # read in global multi-modal transport network
    network_dir = os.path.join(root_dir, "results/multi-modal_network/")
    edges = gpd.read_parquet(os.path.join(network_dir, "edges.gpq"))

    print("Reading OD matrix...")
    # read in trade OD matrix
    od_dir = os.path.join(root_dir, "results/input/trade_matrix")
    od = pd.read_parquet(os.path.join(od_dir, "trade_nodes_total.parquet"))

    # only keep most significant pairs
    # 5t threshold drops THL road -> GID_0 OD from ~21M -> ~2M
    od = od[od.volume_tons > 1]

    routes = route_from_all_nodes(od, edges, n_cpu)

    print("Writing routes to disk as JSON...")
    with open(os.path.join(flow_allocation_dir, 'routes.json'), 'w') as fp:
        json.dump(routes, fp, indent=2)

    print("Assigning route flows to edges...")
    edges["value_kusd"] = 0
    edges["volume_tons"] = 0
    value_col_id = edges.columns.get_loc("value_kusd")
    volume_col_id = edges.columns.get_loc("volume_tons")
    for from_node, from_node_routes in routes.items():
        for destination_country, route_data in from_node_routes.items():
            edge_indicies = route_data["edge_indicies"]
            edges.iloc[edge_indicies, value_col_id] += route_data["value_kusd"]
            edges.iloc[edge_indicies, volume_col_id] += route_data["volume_tons"]

    print("Writing edge flows to disk as geoparquet...")
    flow_allocation_dir = os.path.join(root_dir, "results/flow_allocation/")
    os.makedirs(flow_allocation_dir, exist_ok=True)
    edges_with_flows.to_parquet(os.path.join(flow_allocation_dir, "edges_with_flows.gpq"))

    print("Done")
