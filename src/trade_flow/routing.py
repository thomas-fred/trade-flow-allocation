"""
Allocate flows (value and volume) from an origin-destination (OD) file across a network of edges.
"""

import multiprocessing
import os
import tempfile
import time

import igraph as ig
import geopandas as gpd
import pandas as pd


# dict containing:
# 'value_kusd' -> float
# 'volume_tons' -> float
# 'edge_indicies' -> list[indicies]
FlowResult = dict[str, float | list[int]]

# dict with FlowResult values
# source node, destination country -> FlowResult dict
# e.g.
# ('thailand_4_123', 'GID_0_GBR')  -> FlowResult dict
RouteResult = dict[tuple[str, str], FlowResult]


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
        Mapping from (source node, destination country node) key, to value of
            flow, volume of flow and list of edge ids of route.
    """
    print(f"Process {os.getpid()} routing {from_node}...")

    from_node_od = od[od.id == from_node]
    destination_nodes: list[str] = [f"GID_0_{iso_a3}" for iso_a3 in from_node_od.partner_GID_0.unique()]

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
        else:
            raise error

    routes: RouteResult = {}

    if routes_edge_list:
        assert len(routes_edge_list) == len(destination_nodes)
    else:
        return routes

    # lookup trade value and volume for each pairing of from_node and partner country
    for i, destination_node in enumerate(destination_nodes):
        # "GID_0_GBR" -> "GBR"
        iso_a3 = destination_node.split("_")[-1]
        route = from_node_od[
            (from_node_od.id == from_node) & (from_node_od.partner_GID_0 == iso_a3)
        ]
        value_kusd, = route.value_kusd
        volume_tons, = route.volume_tons

        routes[(from_node, destination_node)] = {
            "value_kusd": value_kusd,
            "volume_tons": volume_tons,
            "edge_indices": routes_edge_list[i]
        }

    print(f"Process {os.getpid()} finished routing {from_node}...")
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
            and volume along this route and list of edge indices constituting
            the route.
    """

    print("Creating graph...")
    # cannot add vertices as edges reference port493_out, port281_in, etc. which are missing from nodes file
    # use_vids=False as edges.from_id and edges_to_id are not integers
    graph = ig.Graph.DataFrame(edges, directed=True, use_vids=False)

    temp_dir = tempfile.TemporaryDirectory()

    print("Writing graph to disk...")
    graph_filepath = os.path.join(temp_dir.name, "graph.pickle")
    graph.write_pickle(graph_filepath)

    print("Writing OD to disk...")
    od_filepath = os.path.join(temp_dir.name, "od.pq")
    od.to_parquet(od_filepath)

    print("Routing...")
    start = time.time()
    from_nodes = od.id.unique()
    args = ((from_node,) for from_node in from_nodes)
    # as each process is created, it will load the graph and od from disk in
    # init_worker and then persist these in memory as globals between chunks
    with multiprocessing.Pool(
        processes=n_cpu,
        initializer=init_worker,
        initargs=(graph_filepath, od_filepath),
    ) as pool:
        routes: list[RouteResult] = pool.starmap(route_from_node, args)

    print("\n")
    print(f"Routing completed in {time.time() - start:.2f}s")

    temp_dir.cleanup()

    # flatten our list of RouteResult dicts into one dict
    return {k: v for item in routes for (k, v) in item.items()}
