"""

"""

rule allocate:
    input:
        edges = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/edges.gpq",
        od = "{OUTPUT_DIR}/input/trade_matrix/{PROJECT}/trade_nodes_total.parquet",
    output:
        routes = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/routes.json",
        edges_with_flows = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/edges.gpq",
    run:
        import json

        import geopandas as gpd
        import pandas as pd

        from trade_flow.routing import route_from_all_nodes

        print("Reading network...")
        # read in global multi-modal transport network
        edges = gpd.read_parquet(input.edges)
        available_destinations = edges[edges["mode"] == "imaginary"].to_id.unique()
        available_country_destinations = [d.split("_")[-1] for d in available_destinations if d.startswith("GID_")]

        print("Reading OD matrix...")
        # read in trade OD matrix
        od = pd.read_parquet(input.od)
        print(f"OD has {len(od):,d} flows")

        # 5t threshold drops THL road -> GID_0 OD from ~21M -> ~2M
        minimum_flow_volume_tons = config["minimum_flow_volume_t"]
        od = od[od.volume_tons > minimum_flow_volume_tons]
        print(f"After dropping flows with volume < {minimum_flow_volume_tons}t, OD has {len(od):,d} flows")

        # drop any flows we can't find a route to
        od = od[od.partner_GID_0.isin(available_country_destinations)]
        print(f"After dropping unrouteable destination countries, OD has {len(od):,d} flows")

        routes = route_from_all_nodes(od, edges, workflow.cores)

        print("Writing routes to disk as JSON...")
        with open(output.routes, 'w') as fp:
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
        edges.to_parquet(output.edges_with_flows)

        print("Done")