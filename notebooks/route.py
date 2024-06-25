import os
import time

import igraph as ig
import geopandas as gpd
import pandas as pd


# source node       -> destination country  -> (value kUSD, volume t,   [integer edge ids])
# thailand_4_123    -> GID_0_GBR            -> (24.2,       4.1,        [432, 7, 123, ...])
RouteResult = dict[str, dict[str, tuple[float, float, list[int]]]]


def route_from_node(from_node: str, od: pd.DataFrame, graph: ig.Graph) -> RouteResult:
    """
    Route flows from single 'from_node' to destinations across graph. Record value and
    volume flowing across each edge.

    Args:
        from_node: Node ID of source node.
        od: Table of flows from origin node 'id' to destination country
            'partner_GID_0', should also contain 'value_kusd' and 'volume_tons'.
        graph: Graph to route over.

    Returns:
        Mapping from source node, to destination country node, to value of
            flow, volume of flow and list of edge ids of route.
    """

    from_node_od = od[od.id == from_node]
    destination_nodes = [f"GID_0_{iso_a3}" for iso_a3 in from_node_od.partner_GID_0.unique()]

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

    assert len(routes_edge_list) == len(destination_nodes)

    routes: RouteResult = {}
    for i, destination_node in enumerate(destination_nodes):

        # lookup trade value and volume for each pairing of from_node and partner country
        # GID_0_GBR -> GBR
        iso_a3 = destination_node.split("_")[-1]
        route = from_node_od[
            (from_node_od.id == from_node) & (from_node_od.partner_GID_0 == iso_a3)
        ]
        value_kusd, = route.value_kusd
        volume_tons, = route.volume_tons

        routes[from_node] = {destination_node: (value_kusd, volume_tons, routes_edge_list[i])}

    return routes


def route_from_all_nodes(od: pd.DataFrame, edges: gpd.GeoDataFrame) -> tuple[RouteResult, gpd.GeoDataFrame]:
    """
    Route flows from origins to destinations across graph. Record value and
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
    from_nodes = od.id.unique()[:30]
    routes = []
    for i, from_node in enumerate(from_nodes):

        routes.append(route_from_node(from_node, od, graph))
        print(f"{i + 1} of {len(from_nodes)}, {from_node}", end="\r")

    print("\r")
    print(f"Routing completed in {time.time() - start:.2f}s")

    print("Assigning flows to edges...")
    # combine our list of singleton dicts into one dict with multiple keys
    routes = {k: v for item in routes for (k, v) in item.items()}
    for from_node, from_node_routes in routes.items():
        for destination_country, route_data in from_node_routes.items():
            value_kusd, volume_t, integer_edge_ids = route_data
            edges.iloc[integer_edge_ids, value_col_id] = value_kusd
            edges.iloc[integer_edge_ids, volume_col_id] = volume_t

    return routes, edges


if __name__ == "__main__":

    root_dir = ".."

    print("Reading network...")
    # read in global multi-modal transport network
    network_dir = os.path.join(root_dir, "results/multi-modal_network/")
    edges = gpd.read_parquet(os.path.join(network_dir, "edges.gpq"))

    print("Reading OD matrix...")
    # read in trade OD matrix
    od_dir = os.path.join(root_dir, "results/input/trade_matrix")
    od = pd.read_parquet(os.path.join(od_dir, "trade_nodes_total.parquet"))

    # only keep most significant pairs, drops number from ~21M -> ~2M
    od = od[od.volume_tons > 5]

    routes, edges_with_flows = route_from_all_nodes(od, edges)

    print("Writing flows to disk...")
    edges_with_flows.to_parquet("edges_with_flows.gpq")

    print("Done")
