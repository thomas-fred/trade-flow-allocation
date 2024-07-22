rule create_maritime_network:
    input:
        nodes = "{OUTPUT_DIR}/input/networks/maritime/nodes.gpq",
        edges_no_geom = "{OUTPUT_DIR}/input/networks/maritime/edges_by_cargo/maritime_base_network_general_cargo.pq",
        edges_visualisation = "{OUTPUT_DIR}/input/networks/maritime/edges.gpq",
    output:
        nodes = "{OUTPUT_DIR}/maritime_network/nodes.gpq",
        edges = "{OUTPUT_DIR}/maritime_network/edges.gpq",
    run:
        import igraph as ig
        import geopandas as gpd
        from shapely.geometry import Point
        from shapely.ops import linemerge
        from tqdm import tqdm

        from trade_flow.network_creation import preprocess_maritime_network

        # possible cargo types = ("container", "dry_bulk", "general_cargo",  "roro", "tanker")
        # for now, just use 'general_cargo'
        maritime_nodes, maritime_edges_no_geom = preprocess_maritime_network(
            input.nodes,
            input.edges_no_geom
        )

        if config["study_country_iso_a3"] == "THA":
            # put Bangkok port in the right place...
            maritime_nodes.loc[maritime_nodes.name == "Bangkok_Thailand", "geometry"] = Point((100.5753, 13.7037))

        # %%
        # Jasper's maritime edges in 'edges_by_cargo' do not contain geometry
        # this is because the AIS data that they were derived from only contain origin and destination port, not route
        # this is a pain for visualisation, so we will create a geometry for each from `maritime_vis_edges`

        maritime_vis_edges = gpd.read_parquet(input.edges_visualisation)
        vis_graph = ig.Graph.DataFrame(maritime_vis_edges, directed=True, use_vids=False)

        maritime_edges = maritime_edges_no_geom.copy()
        change_of_port_mask = maritime_edges_no_geom.from_port != maritime_edges_no_geom.to_port
        port_pairs_to_generate_geom_for = maritime_edges_no_geom[change_of_port_mask]
        for index, row in tqdm(port_pairs_to_generate_geom_for.iterrows(), total=len(port_pairs_to_generate_geom_for)):
            edge_list = vis_graph.get_shortest_path(row.from_port, row.to_port, weights="distance", output="epath") 
            route_edges = maritime_vis_edges.iloc[edge_list]
            route_linestring = linemerge(list(route_edges.geometry))
            maritime_edges.loc[index, "geometry"] = route_linestring

        maritime_edges = gpd.GeoDataFrame(maritime_edges).set_crs(epsg=4326)

        maritime_nodes.to_parquet(output.nodes)
        maritime_edges.to_parquet(output.edges)

rule plot_port_connections:
    input:
        nodes = "{OUTPUT_DIR}/maritime_network/nodes.gpq",
        edges = "{OUTPUT_DIR}/maritime_network/edges.gpq",
    output:
        edges_plot = "{OUTPUT_DIR}/maritime_network/edges.png",
        port_trade_plots = directory("{OUTPUT_DIR}/maritime_network/port_trade_plots"),
    run:
        import os

        import geopandas as gpd
        import matplotlib
        import matplotlib.pyplot as plt
        from tqdm import tqdm

        matplotlib.use("Agg")

        maritime_nodes = gpd.read_parquet(input.nodes)
        maritime_edges = gpd.read_parquet(input.edges)

        # diagnostic plotting
        f, ax = plt.subplots(figsize=(16, 16))
        maritime_edges.to_crs(epsg=3995).plot(
            ax=ax,
            linewidth=0.5,
            alpha=1
        )
        ax.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)
        f.savefig(output.edges_plot)

        # disambiguate the global view and plot the routes from each port, one port at a time
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        world.geometry = world.geometry.boundary

        ports = maritime_edges.from_port.unique()
        stereographic_proj: int = 3995
        os.makedirs(output.port_trade_plots)
        for port_id in tqdm(ports):
            filepath = os.path.join(output.port_trade_plots, f"{port_id}.png")
            f, ax = plt.subplots(figsize=(10,10))
            port = maritime_nodes[maritime_nodes.id == f"{port_id}_land"]
            routes = maritime_edges[maritime_edges.from_port == port_id].to_crs(epsg=stereographic_proj)
            if all(routes.geometry.isna()):
                continue
            routes.plot(
                column="to_port",
                categorical=True,
                ax=ax
            )
            maritime_nodes[maritime_nodes.id == f"{port_id}_land"].to_crs(epsg=stereographic_proj).plot(
                ax=ax,
                markersize=500,
                marker="*",
                facecolor="none",
                color="r"
            )
            xmin, xmax = ax.get_xlim()
            ymin, ymax = ax.get_ylim()
            world.to_crs(epsg=stereographic_proj).plot(ax=ax, linewidth=0.5, alpha=0.4)
            ax.set_xlim(xmin, xmax)
            ax.set_ylim(ymin, ymax)
            port_name, = port.name
            ax.set_title(f"{port_id} ({port_name.replace('_', ', ')}) estimated routes")
            ax.get_xaxis().set_visible(False)
            ax.get_yaxis().set_visible(False)
            
            f.savefig(filepath)
            plt.close(f)