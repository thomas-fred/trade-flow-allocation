rule allocate_intact_network:
    """
    Allocate a trade OD matrix across a multi-modal transport network
    """
    input:
        edges = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/edges.gpq",
        od = "{OUTPUT_DIR}/input/trade_matrix/{PROJECT}/trade_nodes_total.parquet",
    threads: workflow.cores
    params:
        # if this changes, we want to trigger a re-run
        minimum_flow_volume_t = config["minimum_flow_volume_t"],
    output:
        routes = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/routes.pq",
        edges_with_flows = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/edges.gpq",
    script:
        "./allocate.py"


rule allocate_degraded_network:
    """
    Allocate a trade OD matrix across a multi-modal transport network which has
    lost edges as a result of intersection with a hazard map.
    """
    input:
        edges = "{OUTPUT_DIR}/multi-modal_network/{PROJECT}/{HAZARD}/edges.gpq",
        od = "{OUTPUT_DIR}/input/trade_matrix/{PROJECT}/trade_nodes_total.parquet",
    threads: workflow.cores
    params:
        # if this changes, we want to trigger a re-run
        minimum_flow_volume_t = config["minimum_flow_volume_t"],
    output:
        routes = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/{HAZARD}/routes.pq",
        edges_with_flows = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/{HAZARD}/edges.gpq",
    script:
        "./allocate.py"


rule accumulate_route_costs_intact:
    input:
        routes = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/routes.pq",
        edges_with_flows = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/edges.gpq",
    output:
        routes_with_costs = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/routes_with_costs.pq",
    run:
        from trade_flow.routing import lookup_route_costs 

        lookup_route_costs(input.routes, input.edges_with_flows).to_parquet(output.routes_with_costs)



rule accumulate_route_costs_degraded:
    input:
        routes = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/{HAZARD}/routes.pq",
        edges_with_flows = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/{HAZARD}/edges.gpq",
    output:
        routes_with_costs = "{OUTPUT_DIR}/flow_allocation/{PROJECT}/{HAZARD}/routes_with_costs.pq",
    run:
        from trade_flow.routing import lookup_route_costs 

        lookup_route_costs(input.routes, input.edges_with_flows).to_parquet(output.routes_with_costs)
