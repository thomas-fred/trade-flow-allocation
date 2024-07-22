rule create_multi_modal_network:
    """
    Take previously created road, rail and maritime networks and combine them
    into a single multi-modal network with intermodal connections within
    distance limit of: any road node, any rail station and any maritime port.
    """
    input:
        admin_boundaries = "{OUTPUT_DIR}/input/admin-boundaries/admin-level-0.geoparquet",
        road_network_nodes = "{OUTPUT_DIR}/input/networks/road/{PROJECT}/nodes.gpq",
        road_network_edges = "{OUTPUT_DIR}/input/networks/road/{PROJECT}/edges.gpq",
        rail_network_nodes = "{OUTPUT_DIR}/input/networks/rail/{PROJECT}/nodes.gpq",
        rail_network_edges = "{OUTPUT_DIR}/input/networks/rail/{PROJECT}/edges.gpq",
        maritime_nodes = "{OUTPUT_DIR}/maritime_network/nodes.gpq",
        maritime_edges = "{OUTPUT_DIR}/maritime_network/edges.gpq",
    output:
        border_crossing_plot = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/border_crossings.png",
        nodes = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/nodes.gpq",
        edges = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/edges.gpq",
    run:
        import geopandas as gpd
        import numpy as np
        import matplotlib
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
        import pandas as pd

        from trade_flow.network_creation import (
            duplicate_reverse_and_append_edges, preprocess_road_network,
            preprocess_rail_network, create_edges_to_nearest_nodes,
            find_importing_node_id, create_edges_to_destination_countries
        )

        matplotlib.use("Agg")

        study_country: str = config["study_country_iso_a3"]

        road_nodes, road_edges = preprocess_road_network(
            input.road_network_nodes,
            input.road_network_edges,
            {study_country,},
            config["road_cost_USD_t_km"],
            config["road_cost_USD_t_h"],
            True,
            config["road_default_speed_limit_km_h"]
        )

        rail_nodes, rail_edges = preprocess_rail_network(
            input.rail_network_nodes,
            input.rail_network_edges,
            {study_country,},
            config["rail_cost_USD_t_km"],
            config["rail_cost_USD_t_h"],
            True,
            config["rail_average_freight_speed_km_h"]
        )

        maritime_nodes = gpd.read_parquet(input.maritime_nodes) 
        maritime_edges = gpd.read_parquet(input.maritime_edges)

        maximum_intermodal_connection_metres = 2_000

        # road-rail
        rail_road_edges = create_edges_to_nearest_nodes(
            rail_nodes.loc[rail_nodes.station == True, ["id", "iso_a3", "geometry"]],
            road_nodes.loc[:, ["id", "geometry"]],
            maximum_intermodal_connection_metres,
            rail_nodes.estimate_utm_crs()
        ).to_crs(epsg=4326)
        rail_road_edges["mode"] = "road_rail"

        # road-maritime
        maritime_road_edges = create_edges_to_nearest_nodes(
            maritime_nodes.loc[
                (maritime_nodes.infra == "port") & (maritime_nodes.iso_a3 == study_country),
                ["id", "iso_a3", "geometry"]
            ],
            road_nodes.loc[:, ["id", "geometry"]],
            maximum_intermodal_connection_metres,
            road_nodes.estimate_utm_crs()
        ).to_crs(epsg=4326)
        maritime_road_edges["mode"] = "maritime_road"

        # rail-maritime
        maritime_rail_edges = create_edges_to_nearest_nodes(
            maritime_nodes.loc[
                (maritime_nodes.infra == "port") & (maritime_nodes.iso_a3 == study_country),
                ["id", "iso_a3", "geometry"]
            ],
            rail_nodes.loc[rail_nodes.station == True, ["id", "geometry"]],
            maximum_intermodal_connection_metres,
            road_nodes.estimate_utm_crs()
        ).to_crs(epsg=4326)
        maritime_rail_edges["mode"] = "maritime_rail"

        intermodal_edges = pd.concat(
            [
                rail_road_edges,
                maritime_road_edges,
                maritime_rail_edges
            ]
        ).to_crs(epsg=4326)
        # as the maritime edges are directional, we're making road, rail and intermodal directional too (so duplicate)
        intermodal_edges = duplicate_reverse_and_append_edges(intermodal_edges)

        intermodal_cost_USD_t = config["intermodal_cost_USD_t"]
        intermodal_edges["cost_USD_t"] = intermodal_edges["mode"].map(intermodal_cost_USD_t)

        
        # concatenate different kinds of nodes and edges
        node_cols = ["id", "iso_a3", "geometry"]
        nodes = gpd.GeoDataFrame(
            pd.concat(
                [
                    road_nodes.loc[:, node_cols],
                    rail_nodes.loc[:, node_cols],
                    maritime_nodes.loc[:, node_cols]
                ]
            ),
            crs=4326
        )

        edge_cols = ["from_id", "to_id", "from_iso_a3", "to_iso_a3", "mode", "cost_USD_t", "geometry"]
        edges = pd.concat(
            [
                intermodal_edges.loc[:, edge_cols],
                road_edges.loc[:, edge_cols],
                rail_edges.loc[:, edge_cols],
                maritime_edges.loc[:, edge_cols]
            ]
        )
        
        # add nodes for destination countries (not null, not origin country)
        # neighbouring countries will have destination node connected to border crossings
        countries = nodes.iso_a3.unique()
        countries = countries[countries != np.array(None)]
        countries = set(countries)
        countries.remove(study_country)

        admin_boundaries = gpd.read_parquet(input.admin_boundaries)
        country_nodes = admin_boundaries.set_index("GID_0").loc[list(countries), ["geometry"]] \
            .sort_index().reset_index().rename(columns={"GID_0": "iso_a3"})
        country_nodes["id"] = country_nodes.apply(lambda row: f"GID_0_{row.iso_a3}", axis=1)
        country_nodes.geometry = country_nodes.geometry.representative_point()
        destination_country_nodes = country_nodes.loc[:, ["id", "iso_a3", "geometry"]]

        nodes = pd.concat(
            [
                nodes,
                destination_country_nodes
            ]
        )
        
        # find nodes which lie on far side of border crossing
        border_crossing_mask = \
            (edges.from_iso_a3 != edges.to_iso_a3) \
            & ((edges.from_iso_a3 == study_country) | (edges.to_iso_a3 == study_country)) \
            & ((edges["mode"] == "road") | (edges["mode"] == "rail")) \
            
        importing_node_ids = edges[border_crossing_mask].apply(find_importing_node_id, exporting_country=study_country, axis=1)
        importing_nodes = nodes.set_index("id").loc[importing_node_ids].reset_index()
        # two importing nodes are labelled as THA, drop these
        importing_nodes = importing_nodes[importing_nodes.iso_a3 != study_country]

        # connect these nodes to their containing country
        land_border_to_importing_country_edges = \
            create_edges_to_destination_countries(importing_nodes, destination_country_nodes)
        land_border_to_importing_country_edges.plot()
        
        # plot THA land border crossing points for sanity
        to_plot = importing_nodes
        country_ints, labels = pd.factorize(to_plot["iso_a3"])
        unique_country_ints = []
        cmap = plt.get_cmap("viridis")
        colours = [cmap(x) for x in country_ints / max(country_ints)]
        [unique_country_ints.append(c) for c in colours if c not in unique_country_ints]
        colour_map = dict(zip(labels, unique_country_ints))
        f, ax = plt.subplots()
        to_plot.plot(color=colours, ax=ax)
        ax.set_title("Land border crossing points")
        patches = [mpatches.Patch(color=colour, label=label) for label, colour in colour_map.items()]
        ax.legend(handles=patches)
        f.savefig(output.border_crossing_plot)
        
        # connect foreign ports to their country with new edges
        foreign_ports = maritime_nodes[maritime_nodes.infra=="port"]
        foreign_ports = foreign_ports[foreign_ports.iso_a3 != study_country]
        port_to_importing_countries_edges = create_edges_to_destination_countries(foreign_ports, destination_country_nodes)

        # add in edges connecting destination countries to THA land borders and foreign ports
        edges = pd.concat(
            [
                edges.loc[:, edge_cols],
                duplicate_reverse_and_append_edges(land_border_to_importing_country_edges.loc[:, edge_cols]),
                duplicate_reverse_and_append_edges(port_to_importing_countries_edges.loc[:, edge_cols]),
            ]
        ).reset_index(drop=True)

        # there are duplicate edges (repeated from_id -> to_id pairs), drop these here
        edges["unique_edge_id"] = edges.apply(lambda row: f"{row.from_id}_{row.to_id}", axis=1)
        edges = edges[~edges.unique_edge_id.duplicated(keep="first")].drop(columns=["unique_edge_id"])

        # write out global multi-modal transport network to disk
        # reset indicies to 0-start integers
        # these will correspond to igraph's internal edge/vertex ids
        nodes = nodes.reset_index(drop=True)
        nodes.to_parquet(output.nodes)
        edges = edges.reset_index(drop=True)
        edges.to_parquet(output.edges)