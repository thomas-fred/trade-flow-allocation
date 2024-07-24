configfile: "config.yaml"

wildcard_constraints:
    # the output dir must end in 'results'
    # e.g. '/data/open-gira/results', './results', '/my-results', etc.
    # this prevents us matching past it into other folders for an incorrect OUTPUT_DIR,
    # but is more flexible than reading a value from, for example, config.yaml
    OUTPUT_DIR="^.*results",
    PROJECT="project-[^_/]+",
    HAZARD="hazard-[^_/]+",
    CHUNK="chunk-[\d]+",

include: "workflow/network_creation/maritime.smk"
include: "workflow/network_creation/multi_modal.smk"
include: "workflow/flow_allocation/allocate.smk"