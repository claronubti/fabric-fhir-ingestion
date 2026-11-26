# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # NB_02_Bronze_Extract (pure Spark)
# Pulls one FHIR resource into Bronze.
# 
# 1. Reads config and watermark from `WH_Metadata` (Spark connector).
# 2. Calls the API (full or incremental), following `link.next` page by page.
# 3. Saves each page's raw JSON to `Files/bronze/fhir/{resource}/load_date=.../run_id=.../page_0001.json`.
# 4. Appends rows to the Delta table `bronze_{resource}` (id, lastUpdated, raw_json, run_id, ingest_ts, source_url).
# 5. Appends a row to `ctl_run_log`; for a full, uncapped load it reconciles against `_summary=count`.
# 6. Returns `max_lastUpdated` to the pipeline. **It never moves the watermark** (Silver does that after it succeeds).
# 
# **Before running:** import `NB_Common_Utils` into the same workspace and attach **LH_Bronze as the default lakehouse** of this notebook.

# PARAMETERS CELL ********************

# PARAMETERS
resource_name   = "Patient"
run_id          = ""        # blank = generate one
pipeline_run_id = ""        # filled by the pipeline
load_type       = ""        # blank = use ctl_resource_config.load_type; or FULL / INCREMENTAL
max_pages       = 5         # 0 = no cap (use 0 in Prod). Keep small in Dev.


# CELL ********************

%run NB_Common_Utils

# CELL ********************

import os, json

run_id          = run_id or str(uuid.uuid4())
pipeline_run_id = pipeline_run_id or "manual"
max_pages       = int(max_pages or 0)

cfg        = get_config(resource_name)
wm         = get_watermark(cfg["config_id"])                   # naive UTC datetime
eff_load   = (load_type or cfg["load_type"] or "FULL").upper()
table_name = f"bronze_{resource_name.lower()}"
load_date  = utc_now().strftime("%Y-%m-%d")
out_dir    = f"/lakehouse/default/Files/bronze/fhir/{resource_name}/load_date={load_date}/run_id={run_id}"

assert os.path.isdir("/lakehouse/default/Files"), "Attach LH_Bronze as the default lakehouse of this notebook."
log.info("resource=%s load_type=%s watermark=%s max_pages=%s run_id=%s", resource_name, eff_load, wm, max_pages, run_id)

BRONZE_SCHEMA = StructType([
    StructField("id",          StringType(),    False),
    StructField("lastUpdated", TimestampType(), True),
    StructField("raw_json",    StringType(),    True),
    StructField("run_id",      StringType(),    True),
    StructField("ingest_ts",   TimestampType(), True),
    StructField("source_url",  StringType(),    True),
])
FLUSH_EVERY = 5000

# CELL ********************

def flush(rows):
    """Append buffered rows to the Bronze Delta table."""
    if not rows:
        return 0
    spark.createDataFrame(rows, BRONZE_SCHEMA).write.format("delta").mode("append").saveAsTable(table_name)
    return len(rows)

def extract():
    base = cfg["base_url"].rstrip("/")
    url  = f"{base}/{cfg['endpoint'] or resource_name}"
    params = {"_count": cfg["page_size"], "_sort": "_lastUpdated"}
    if eff_load == "INCREMENTAL":
        params[cfg["incremental_param"]] = "gt" + to_fhir_param(wm)

    os.makedirs(out_dir, exist_ok=True)
    buffer, read, written, pages, max_lu = [], 0, 0, 0, None

    for page, bundle in paginate(url, params, max_pages or None):
        pages = page
        with open(f"{out_dir}/page_{page:04d}.json", "w", encoding="utf-8") as f:
            json.dump(bundle, f)
        for e in bundle.get("entry", []):
            if e.get("search", {}).get("mode") == "outcome":      # skip OperationOutcome entries
                continue
            res = e.get("resource") or {}
            if "id" not in res:
                continue
            lu = parse_fhir_ts(res.get("meta", {}).get("lastUpdated"))
            buffer.append((res["id"], lu, json.dumps(res), run_id, utc_now(), e.get("fullUrl")))
            read += 1
            if lu and (max_lu is None or lu > max_lu):
                max_lu = lu
            if len(buffer) >= FLUSH_EVERY:
                written += flush(buffer); buffer = []
        log.info("page %d done, %d rows so far", page, read)
    written += flush(buffer)

    # Reconcile against the server count for an uncapped full load
    if eff_load == "FULL" and not max_pages:
        total = call_api(url, {"_summary": "count"}).get("total")
        log.info("Reconcile: loaded %d, server reports %s", read, total)
        if total is not None and abs(total - read) > max(10, 0.01 * total):
            log.warning("Row count differs from server total (public data changes); check manually.")

    return {"read": read, "written": written, "pages": pages, "max_lu": max_lu}

result = run_step("BRONZE", cfg, run_id, pipeline_run_id, extract)
log.info("Bronze done: %s", {k: (str(v) if k == "max_lu" else v) for k, v in result.items()})

# CELL ********************

# Return values to the pipeline (the watermark is NOT updated here)
max_lu_str = to_fhir_param(result["max_lu"]) if result["max_lu"] else None
notebookutils.notebook.exit(json.dumps({
    "resource": resource_name,
    "run_id": run_id,
    "rows": result["written"],
    "pages": result["pages"],
    "max_lastUpdated": max_lu_str,
}))
