import collections
import os
import time

import igraph as ig
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import shapely
from tqdm import tqdm


# source_node -> destination country e.g. GID_0_GBR -> list of integer edge ids
RouteResult = dict[str, dict[str, list[int]]]


def route(od: pd.DataFrame, edges: gpd.GeoDataFrame) -> tuple[RouteResult, gpd.GeoDataFrame]:
    """
    Route flows from origin to destination across graph. Record value and
    volume flowing across each edge.

    Args:
        od: Table of flows from origin node 'id' to destination country
            'partner_GID_0', should also contain 'value_kusd' and 'volume_tons'.
        edges: Table of edges to construct graph from. First column should be
            source node id and second destination node id.

    Returns:
        Mapping from source node, to destination country node, to list of edge
            ids connecting them.
        Mutated edge table with 'value_kusd' and 'volume_tons' columns with
            accumulated flows.
    """

    print("Creating graph...")
    # cannot add vertices as edges reference port493_out, port281_in, etc. which are missing from nodes file
    # use_vids=False as edges.from_id and edges_to_id are not integers
    graph = ig.Graph.DataFrame(edges, directed=True, use_vids=False)

    edges["value_kusd"] = 0
    edges["volume_tons"] = 0
    value_col_id = edges.columns.get_loc("value_kusd")
    volume_col_id = edges.columns.get_loc("volume_tons")

    print("Routing...")
    start = time.time()
    routes: RouteResult = {}
    from_nodes = od.id.unique()[:100]
    for i, from_node in enumerate(from_nodes):
        print(f"{i + 1} of {len(from_nodes)}, {from_node}", end="\r")

        from_node_od = od[od.id == from_node]
        dest_nodes = [f"GID_0_{iso_a3}" for iso_a3 in from_node_od.partner_GID_0.unique()]

        try:
            routes_edge_list: list[list[int]] = graph.get_shortest_paths(
                f"road_{from_node}",
                dest_nodes,
                weights="cost_USD_t",
                output="epath"
            )
        except ValueError as error:
            if "no such vertex" in str(error):
                print(f"{error}... skipping destination")
                continue

        # store routes
        routes[from_node] = dict(zip(dest_nodes, routes_edge_list))

        for dest_node, route_edge_ids in routes[from_node].items():

            # lookup trade value and volume for each pairing of from_node and partner country
            # GID_0_GBR -> GBR
            iso_a3 = dest_node.split("_")[-1]
            route = from_node_od[
                (from_node_od.id == from_node) & (from_node_od.partner_GID_0 == iso_a3)
            ]
            value_kusd, = route.value_kusd
            volume_tons, = route.volume_tons

            # increment edges with flows
            edges.iloc[route_edge_ids, value_col_id] += value_kusd
            edges.iloc[route_edge_ids, volume_col_id] += volume_tons

    print(f"Routing completed in {time.time() - start:.2f}s")

    return routes, edges


if __name__ == "__main__":

    root_dir = ".."

    print("Reading network...")
    # read in global multi-modal transport network
    network_dir = os.path.join(root_dir, "results/multi-modal_network/")
    nodes = gpd.read_parquet(os.path.join(network_dir, "nodes.gpq"))
    edges = gpd.read_parquet(os.path.join(network_dir, "edges.gpq"))

    print("Reading OD matrix...")
    # read in trade OD matrix
    od_dir = os.path.join(root_dir, "results/input/trade_matrix")
    od = pd.read_parquet(os.path.join(od_dir, "trade_nodes_total.parquet"))

    # only keep most significant pairs, drops number from ~21M -> ~2M
    od = od[od.volume_tons > 5]

    routes: RouteResult, edges_with_flows: gpd.GeoDataFrame = route(od, edges)

    print("Writing flows to disk...")
    edges_with_flows.to_parquet("edges_with_flows.gpq")

    print("Done")
