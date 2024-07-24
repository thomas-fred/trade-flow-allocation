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
    script:
        "./multi_modal.py"


rule remove_edges_due_to_hazard:
    """
    Take a multi-modal network and remove edges that experience hazard values in
    excess of a given threshold.
    """
    input:
        nodes = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/nodes.gpq",
        edges = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/edges.gpq",
        raster = "{OUTPUT_DIR}/hazard/{HAZARD}.tif",
    output:
        nodes = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/{HAZARD}/nodes.gpq",
        edges = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/{HAZARD}/edges.gpq",
    run:
        import os
        os.environ["SNAIL_PROGRESS"] = "1"

        import geopandas as gpd
        import pandas as pd

        from trade_flow.disruption import filter_edges_by_raster

        nodes = gpd.read_parquet(input.nodes)
        nodes.to_parquet(output.nodes)
        del nodes

        edges = gpd.read_parquet(input.edges)
        land_mask = edges["mode"].isin({"road", "rail"})
        land_edges = edges.loc[land_mask, :].copy()
        land_edges_post_hazard = filter_edges_by_raster(land_edges, input.raster, float(config["edge_failure_threshold"]))
        edges = pd.concat([edges.loc[~land_mask, :], land_edges_post_hazard])
        edges.to_parquet(output.edges)
