# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "dc7fd13c-0d0a-ae8a-4a7a-05da02a2f9e8",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # NB_04_Schema_Evaluation (pure Spark)
# Runs **before Silver** in the pipeline. Samples this run's Bronze JSON, flattens it to `column_path -> data_type`, and compares it with `ctl_schema_registry`.
# 
# | Finding | Meaning | Action |
# |---|---|---|
# | `NEW_COLUMN` | path not in the registry | registered; `on_new_column` = `AUTO_ADD` or `REVIEW` (flag for a person) |
# | `TYPE_CHANGE` | type differs from the registry | `on_type_change` = `WARN` (continue) or `FAIL` (stop the run) |
# | `DROPPED` | registered path missing from the sample | warning only (FHIR fields are optional) |
# 
# Every change is appended to `ctl_schema_change_log`. The registry is append-only (the Spark connector cannot UPDATE): the **latest row per path wins**. The first run for a resource is a baseline: all paths are registered and one `BASELINE` row is logged.
# 
# Types are simplified to `string`, `number` and `boolean`, so `5` vs `5.5` is not a type change. Silver keeps unknown *top-level* fields in `_extra_json`; new fields nested inside known objects stay in Bronze `raw_json` until you promote them.


# PARAMETERS CELL ********************

# PARAMETERS
resource_name    = "Encounter"
run_id           = ""          # Bronze run to sample; blank = latest Bronze run
pipeline_run_id  = ""
on_type_change   = "WARN"      # WARN or FAIL
on_new_column    = "AUTO_ADD"  # AUTO_ADD or REVIEW
sample_size      = 2000
persist_dropped  = False       # True = also write DROPPED rows to the change log (can be noisy)
bronze_lakehouse = "LH_Bronze"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Common_Utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_run_id = pipeline_run_id or "manual"
rl = resource_name.lower()

def lh_path(lakehouse):
    return notebookutils.lakehouse.get(lakehouse)["properties"]["abfsPath"]

def table_path(lakehouse, table):
    """Works for normal lakehouses (Tables/<t>) and schema-enabled ones (Tables/dbo/<t>)."""
    root = f"{lh_path(lakehouse)}/Tables"
    try:
        names = [f.name.rstrip("/") for f in notebookutils.fs.ls(root)]
    except Exception:
        names = []
    if table in names: return f"{root}/{table}"
    if "dbo" in names: return f"{root}/dbo/{table}"
    return f"{root}/{table}"

cfg = get_config(resource_name)
cid = cfg["config_id"]
bronze_path = table_path(bronze_lakehouse, f"bronze_{rl}")
if not notebookutils.fs.exists(bronze_path):
    raise RuntimeError(f"Bronze table not found at {bronze_path}. Run NB_02_Bronze_Extract first.")

bronze_all = spark.read.format("delta").load(bronze_path)
if not run_id:
    last = bronze_all.orderBy(F.col("ingest_ts").desc()).select("run_id").first()
    if last is None:
        raise RuntimeError(f"bronze_{rl} is empty.")
    run_id = last["run_id"]
log.info("resource=%s run_id=%s on_type_change=%s on_new_column=%s", resource_name, run_id, on_type_change, on_new_column)

CHANGE_SCHEMA = StructType([
    StructField("change_id",    LongType(),      False),
    StructField("config_id",    IntegerType(),   True),
    StructField("column_path",  StringType(),    True),
    StructField("change_type",  StringType(),    True),
    StructField("old_type",     StringType(),    True),
    StructField("new_type",     StringType(),    True),
    StructField("detected_at",  TimestampType(), True),
    StructField("action_taken", StringType(),    True),
])
REGISTRY_SCHEMA = StructType([
    StructField("config_id",   IntegerType(),   True),
    StructField("column_path", StringType(),    True),
    StructField("data_type",   StringType(),    True),
    StructField("first_seen",  TimestampType(), True),
    StructField("last_seen",   TimestampType(), True),
    StructField("is_active",   BooleanType(),   True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Flatten a sample of this run's raw JSON to path -> type
def jtype(v):
    if isinstance(v, bool): return "boolean"
    if isinstance(v, (int, float)): return "number"
    return "string"

def walk(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from walk(v, f"{path}.{k}" if path else k)
    elif isinstance(obj, list):
        for v in obj:
            yield from walk(v, f"{path}[]")
    elif obj is not None:
        yield path, jtype(obj)

def observe():
    rows = (bronze_all.where(F.col("run_id") == run_id).select("raw_json").limit(int(sample_size)).collect())
    seen = {}
    for (raw,) in rows:
        for p, t in walk(json.loads(raw)):
            if p.startswith("text."):          # narrative HTML, not data
                continue
            seen.setdefault(p, set()).add(t)
    return len(rows), {p: "|".join(sorted(ts)) for p, ts in seen.items()}

# 2. Latest registry row per path (append-only table: newest last_seen wins)
def load_registry():
    allr = read_meta("ctl_schema_registry").where(F.col("config_id") == cid)
    w = Window.partitionBy("column_path").orderBy(F.col("last_seen").desc())
    latest = allr.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return {r["column_path"]: r.asDict() for r in latest.collect()}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def evaluate():
    n_sample, observed = observe()
    if n_sample == 0:
        log.info("No Bronze rows for run %s; nothing to evaluate", run_id)
        return {"read": 0, "written": 0, "summary": {}}

    registry_all = load_registry()
    active = {p: r for p, r in registry_all.items() if r["is_active"]}
    now = utc_now()
    changes, reg_rows = [], []
    base_id = int(time.time() * 1_000_000)

    def add_change(path, ctype, old, new, action):
        changes.append((base_id + len(changes), cid, path, ctype, old, new, now, action))

    baseline = len(registry_all) == 0
    type_changes = []

    if baseline:
        for p, t in observed.items():
            reg_rows.append((cid, p, t, now, now, True))
        add_change("*", "BASELINE", None, f"{len(observed)} paths", "REGISTERED")
        log.info("Baseline: registered %d paths for %s", len(observed), resource_name)
    else:
        for p, t in observed.items():
            if p not in active:
                action = "AUTO_ADDED" if on_new_column == "AUTO_ADD" else "REVIEW_REQUIRED"
                first = registry_all[p]["first_seen"] if p in registry_all else now      # re-appearing path keeps first_seen
                add_change(p, "NEW_COLUMN", None, t, action)
                reg_rows.append((cid, p, t, first, now, True))
            elif active[p]["data_type"] != t:
                type_changes.append(p)
                if on_type_change == "FAIL":
                    add_change(p, "TYPE_CHANGE", active[p]["data_type"], t, "RUN_FAILED")   # registry NOT updated
                else:
                    add_change(p, "TYPE_CHANGE", active[p]["data_type"], t, "WARNED")
                    reg_rows.append((cid, p, t, active[p]["first_seen"], now, True))
        dropped = [p for p in active if p not in observed]
        if dropped:
            log.warning("%d registered path(s) not in this sample (optional FHIR fields are normal): %s", len(dropped), dropped[:10])
            if persist_dropped:
                for p in dropped:
                    add_change(p, "DROPPED", active[p]["data_type"], None, "WARNED")

    if changes:
        write_meta(spark.createDataFrame(changes, CHANGE_SCHEMA), "ctl_schema_change_log")
    if reg_rows:
        write_meta(spark.createDataFrame(reg_rows, REGISTRY_SCHEMA), "ctl_schema_registry")

    summary = {}
    for c in changes:
        summary[c[3]] = summary.get(c[3], 0) + 1
    log.info("Schema evaluation for %s: %s", resource_name, summary or "no changes")

    if type_changes and on_type_change == "FAIL":
        raise RuntimeError(f"TYPE_CHANGE detected for {resource_name}: {type_changes[:10]} (on_type_change=FAIL). "
                           "Review, then set on_type_change=WARN or insert the new type into ctl_schema_registry.")
    return {"read": n_sample, "written": len(changes), "summary": summary}

result = run_step("SCHEMA", cfg, run_id, pipeline_run_id, evaluate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show what was recorded for this resource and hand the summary to the pipeline
display(read_meta("ctl_schema_change_log").where(F.col("config_id") == cid).orderBy(F.col("detected_at").desc()).limit(20))
notebookutils.notebook.exit(json.dumps({
    "resource": resource_name, "run_id": run_id,
    "sampled_rows": result["read"], "changes": result["written"], "summary": result["summary"],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
