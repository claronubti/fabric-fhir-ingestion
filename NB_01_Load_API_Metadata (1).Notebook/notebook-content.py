# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # NB_01_Load_API_Metadata (pure Spark)
# Reads the FHIR server's `/metadata` (CapabilityStatement) and fills `ctl_resource_config` and `ctl_watermark` in `WH_Metadata` using Spark only.
# 
# - Resources, endpoints, search parameters and interactions come **from the API**.
# - Only your own choices (which resources, load order, critical flag, partition column) are in the settings cell.
# - **Insert-only and safe to re-run:** resources already in `ctl_resource_config` are left untouched (differences are reported in the log); new ones are appended with a `1900-01-01` watermark.
# 
# Run order: import `NB_Common_Utils` first, then this notebook.

# PARAMETERS CELL ********************

# PARAMETERS
source_id = 1


# CELL ********************

%run NB_Common_Utils

# CELL ********************

# YOUR CHOICES: only things the API cannot decide for you.
# kind: "dim" or "fact" (drives the gold_proc name). partition: None = no partitioning.
SELECTED = {
    "Organization": {"order": 1, "critical": True,  "kind": "dim",  "partition": None},
    "Practitioner": {"order": 2, "critical": True,  "kind": "dim",  "partition": None},
    "Patient":      {"order": 3, "critical": True,  "kind": "dim",  "partition": None},
    "Encounter":    {"order": 4, "critical": False, "kind": "fact", "partition": "lastUpdated_year_month"},
    "Observation":  {"order": 5, "critical": False, "kind": "fact", "partition": "lastUpdated_year_month"},
    "Condition":    {"order": 6, "critical": False, "kind": "fact", "partition": "lastUpdated_year_month"},
}
# FHIR standards applied by rule to every resource
PRIMARY_KEY        = "id"
INCREMENTAL_COLUMN = "meta.lastUpdated"
INCREMENTAL_PARAM  = "_lastUpdated"

# CELL ********************

# 1. Source row and /metadata
src = read_meta("ctl_api_source").where((F.col("source_id") == source_id) & (F.col("is_active") == True)).first()
if src is None:
    raise RuntimeError(f"No active row in ctl_api_source for source_id={source_id}. Run 02_seed_source.sql first.")
base_url = src["base_url"].rstrip("/")
log.info("Source: %s (page size %s)", base_url, src["page_size"])

cap = call_api(f"{base_url}/metadata")
assert cap.get("resourceType") == "CapabilityStatement", "Unexpected response from /metadata"

catalog = {}
for res in cap["rest"][0]["resource"]:
    catalog[res["type"]] = {
        "search_params": sorted(p["name"] for p in res.get("searchParam", [])),
        "interactions":  sorted(i["code"] for i in res.get("interaction", [])),
    }
log.info("API exposes %d resource types", len(catalog))

# 2. Validate the selection against what the API supports
valid = {}
for name, opts in SELECTED.items():
    info = catalog.get(name)
    if not info:
        log.error("%s: NOT exposed by the API, skipped", name); continue
    if "search-type" not in info["interactions"] or "read" not in info["interactions"]:
        log.error("%s: needs search-type and read, has %s, skipped", name, info["interactions"]); continue
    if INCREMENTAL_PARAM not in info["search_params"]:
        log.warning("%s: search parameter %s not listed, incremental loads may not work", name, INCREMENTAL_PARAM)
    valid[name] = {**opts, **info}
    log.info("%s OK: %d search params, interactions %s", name, len(info["search_params"]), info["interactions"])
if not valid:
    raise RuntimeError("No valid resources to load.")

# CELL ********************

# 3. Append new resources to ctl_resource_config (existing rows are not modified)
CONFIG_SCHEMA = StructType([
    StructField("config_id",          IntegerType(), False),
    StructField("source_id",          IntegerType(), True),
    StructField("resource_name",      StringType(),  True),
    StructField("endpoint",           StringType(),  True),
    StructField("primary_key",        StringType(),  True),
    StructField("incremental_column", StringType(),  True),
    StructField("incremental_param",  StringType(),  True),
    StructField("load_type",          StringType(),  True),
    StructField("silver_table",       StringType(),  True),
    StructField("partition_column",   StringType(),  True),
    StructField("gold_proc",          StringType(),  True),
    StructField("load_order",         IntegerType(), True),
    StructField("is_critical",        BooleanType(), True),
    StructField("is_active",          BooleanType(), True),
])

cfg_all  = read_meta("ctl_resource_config")
existing = {r["resource_name"]: r.asDict() for r in cfg_all.where(F.col("source_id") == source_id).collect()}
next_id  = (cfg_all.agg(F.max("config_id")).first()[0] or 0) + 1

new_rows = []
for name, o in sorted(valid.items(), key=lambda kv: kv[1]["order"]):
    desired = {"endpoint": name, "primary_key": PRIMARY_KEY, "incremental_column": INCREMENTAL_COLUMN,
               "incremental_param": INCREMENTAL_PARAM, "silver_table": "silver_" + name.lower(),
               "partition_column": o["partition"], "gold_proc": f"usp_Load_{o['kind']}_{name.lower()}",
               "load_order": o["order"], "is_critical": o["critical"]}
    if name in existing:
        diffs = {k: (existing[name].get(k), v) for k, v in desired.items() if existing[name].get(k) != v}
        if diffs:
            log.warning("%s exists and differs from the settings (left unchanged; edit with SQL if intended): %s", name, diffs)
        else:
            log.info("%s already configured, no change", name)
        continue
    new_rows.append((next_id, source_id, name, desired["endpoint"], PRIMARY_KEY, INCREMENTAL_COLUMN, INCREMENTAL_PARAM,
                     "FULL", desired["silver_table"], desired["partition_column"], desired["gold_proc"],
                     desired["load_order"], desired["is_critical"], True))
    next_id += 1

if new_rows:
    write_meta(spark.createDataFrame(new_rows, CONFIG_SCHEMA), "ctl_resource_config")
log.info("ctl_resource_config: %d resource(s) appended", len(new_rows))

# 4. Seed a 1900-01-01 watermark for every config that has none (first run = full load)
cfg_ids = [r["config_id"] for r in read_meta("ctl_resource_config").where(F.col("source_id") == source_id).collect()]
have    = {r["config_id"] for r in read_meta("ctl_watermark").select("config_id").distinct().collect()}
seed    = [(int(c), datetime(1900, 1, 1), None, utc_now()) for c in cfg_ids if c not in have]
if seed:
    write_meta(spark.createDataFrame(seed, WATERMARK_SCHEMA), "ctl_watermark")
log.info("Watermarks seeded for %d config(s)", len(seed))

# CELL ********************

# 5. Verify
cfg = (read_meta("ctl_resource_config").where(F.col("source_id") == source_id).orderBy("load_order")
       .select("config_id", "resource_name", "endpoint", "primary_key", "incremental_param", "load_type",
               "silver_table", "partition_column", "gold_proc", "load_order", "is_critical", "is_active"))
display(cfg)
display(latest_watermarks().orderBy("config_id"))
display(spark.createDataFrame([(k, ", ".join(v["interactions"]), len(v["search_params"])) for k, v in valid.items()],
                              ["resource", "interactions", "search_param_count"]))
assert cfg.count() >= len(valid), "Fewer config rows than selected resources"
