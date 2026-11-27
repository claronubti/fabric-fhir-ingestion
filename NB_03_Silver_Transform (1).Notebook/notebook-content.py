# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # NB_03_Silver_Transform (pure Spark)
# Turns one run of Bronze into typed, deduplicated, MERGEd Silver rows.
# 
# 1. Reads this run's rows from `bronze_{resource}` (blank `run_id` = the latest Bronze run).
# 2. Parses the raw JSON into typed columns; every other top-level field is kept in `_extra_json`.
# 3. Deduplicates on `id`, keeping the newest `lastUpdated`.
# 4. Adds `lastUpdated_year_month` when the config names a partition column (event-like tables only).
# 5. First run: creates the Silver table. Later runs: Delta **MERGE** on `id`, updating only when the incoming `lastUpdated` is newer.
# 6. Logs to `ctl_run_log`, then **moves the watermark** (only after the MERGE succeeded, so a failure re-reads the same window safely).
# 
# Tables are read and written by **path** (no default-lakehouse catalog needed), so Bronze and Silver can be different lakehouses.
# Run order: `NB_Common_Utils` imported, `NB_02_Bronze_Extract` already run for this resource.

# PARAMETERS CELL ********************

# PARAMETERS
resource_name    = "Practitioner"
run_id           = ""       # Bronze run_id to process; blank = latest Bronze run for this resource
pipeline_run_id  = ""
max_lastUpdated  = ""       # from the Bronze notebook (e.g. 2026-11-25T10:49:17.579Z); blank = computed here
bronze_lakehouse = "LH_Bronze"
silver_lakehouse = "LH_Silver"


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

from delta.tables import DeltaTable

spark.conf.set("spark.sql.ansi.enabled", "false")                       # empty arrays / bad dates give NULL instead of errors
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

pipeline_run_id = pipeline_run_id or "manual"
rl              = resource_name.lower()

def lh_path(lakehouse):
    return notebookutils.lakehouse.get(lakehouse)["properties"]["abfsPath"]

def table_path(lakehouse, table):
    """Works for both normal lakehouses (Tables/<t>) and schema-enabled ones (Tables/dbo/<t>)."""
    root = f"{lh_path(lakehouse)}/Tables"
    try:
        names = [f.name.rstrip("/") for f in notebookutils.fs.ls(root)]
    except Exception:
        names = []
    if table in names:
        return f"{root}/{table}"
    if "dbo" in names:                      # schema-enabled lakehouse
        return f"{root}/dbo/{table}"
    return f"{root}/{table}"

cfg         = get_config(resource_name)
bronze_path = table_path(bronze_lakehouse, f"bronze_{rl}")
silver_path = table_path(silver_lakehouse, cfg["silver_table"])
part_col    = cfg["partition_column"]

if not notebookutils.fs.exists(bronze_path):
    root = f"{lh_path(bronze_lakehouse)}/Tables"
    try:
        found = [f.name for f in notebookutils.fs.ls(root)]
    except Exception:
        found = []
    raise RuntimeError(f"Bronze table not found at {bronze_path}. Tables currently in {bronze_lakehouse}: {found}. "
                       f"Run NB_02_Bronze_Extract for {resource_name} with {bronze_lakehouse} as its default lakehouse.")

bronze_all = spark.read.format("delta").load(bronze_path)
if not run_id:
    last = bronze_all.orderBy(F.col("ingest_ts").desc()).select("run_id").first()
    if last is None:
        raise RuntimeError(f"bronze_{rl} is empty. Run NB_02_Bronze_Extract first.")
    run_id = last["run_id"]
bronze = bronze_all.where(F.col("run_id") == run_id)
log.info("resource=%s run_id=%s bronze=%s silver=%s partition=%s", resource_name, run_id, bronze_path, silver_path, part_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- per-resource typing: schema of the fields we flatten, the columns we produce, the top-level keys we consume ----
def ref_id(c):
    """'Patient/123' or 'http://.../Patient/123' -> '123'"""
    return F.when(c.isNotNull(), F.regexp_extract(c, r"([^/]+)$", 1))

NAME  = "name array<struct<family:string,given:array<string>>>"
ADDR  = "address array<struct<city:string,country:string>>"
META  = "meta struct<lastUpdated:string>"
CODEABLE = "struct<coding:array<struct<system:string,code:string,display:string>>,text:string>"
REF   = "struct<reference:string>"

SPECS = {
 "Patient": {
   "ddl":  f"id string, gender string, birthDate string, {NAME}, {ADDR}, {META}",
   "cols": lambda: [F.col("r.gender").alias("gender"),
                    F.to_date("r.birthDate").alias("birthDate"),
                    F.col("r.name")[0]["family"].alias("family"),
                    F.col("r.name")[0]["given"][0].alias("given"),
                    F.col("r.address")[0]["city"].alias("city"),
                    F.col("r.address")[0]["country"].alias("country")],
   "known": ["gender", "birthDate", "name", "address"]},
 "Practitioner": {
   "ddl":  f"id string, gender string, {NAME}, {META}",
   "cols": lambda: [F.col("r.gender").alias("gender"),
                    F.col("r.name")[0]["family"].alias("family"),
                    F.col("r.name")[0]["given"][0].alias("given")],
   "known": ["gender", "name"]},
 "Organization": {
   "ddl":  f"id string, name string, active boolean, {ADDR}, {META}",
   "cols": lambda: [F.col("r.name").alias("name"),
                    F.col("r.active").alias("active"),
                    F.col("r.address")[0]["city"].alias("city"),
                    F.col("r.address")[0]["country"].alias("country")],
   "known": ["name", "active", "address"]},
 "Encounter": {
   "ddl":  f"id string, status string, `class` struct<system:string,code:string,display:string>, subject {REF}, "
           f"serviceProvider {REF}, period struct<start:string,`end`:string>, "
           f"participant array<struct<individual:struct<reference:string>>>, {META}",
   "cols": lambda: [F.col("r.status").alias("status"),
                    F.col("r.class.code").alias("class_code"),
                    F.col("r.class.display").alias("class_display"),
                    ref_id(F.col("r.subject.reference")).alias("patient_id"),
                    ref_id(F.col("r.serviceProvider.reference")).alias("organization_id"),
                    ref_id(F.col("r.participant")[0]["individual"]["reference"]).alias("practitioner_id"),
                    F.to_timestamp("r.period.start").alias("period_start"),
                    F.to_timestamp("r.period.end").alias("period_end")],
   "known": ["status", "class", "subject", "serviceProvider", "period", "participant"]},
 "Observation": {
   "ddl":  f"id string, status string, code {CODEABLE}, subject {REF}, encounter {REF}, effectiveDateTime string, "
           f"valueQuantity struct<value:double,unit:string>, valueString string, {META}",
   "cols": lambda: [F.col("r.status").alias("status"),
                    F.col("r.code.coding")[0]["system"].alias("code_system"),
                    F.col("r.code.coding")[0]["code"].alias("code"),
                    F.col("r.code.coding")[0]["display"].alias("code_display"),
                    F.col("r.code.text").alias("code_text"),
                    ref_id(F.col("r.subject.reference")).alias("patient_id"),
                    ref_id(F.col("r.encounter.reference")).alias("encounter_id"),
                    F.to_timestamp("r.effectiveDateTime").alias("effective_ts"),
                    F.col("r.valueQuantity.value").alias("value_numeric"),
                    F.col("r.valueQuantity.unit").alias("value_unit"),
                    F.col("r.valueString").alias("value_string")],
   "known": ["status", "code", "subject", "encounter", "effectiveDateTime", "valueQuantity", "valueString"]},
 "Condition": {
   "ddl":  f"id string, clinicalStatus struct<coding:array<struct<code:string>>>, "
           f"verificationStatus struct<coding:array<struct<code:string>>>, code {CODEABLE}, subject {REF}, encounter {REF}, "
           f"onsetDateTime string, recordedDate string, {META}",
   "cols": lambda: [F.col("r.clinicalStatus.coding")[0]["code"].alias("clinical_status"),
                    F.col("r.verificationStatus.coding")[0]["code"].alias("verification_status"),
                    F.col("r.code.coding")[0]["system"].alias("code_system"),
                    F.col("r.code.coding")[0]["code"].alias("code"),
                    F.col("r.code.coding")[0]["display"].alias("code_display"),
                    F.col("r.code.text").alias("code_text"),
                    ref_id(F.col("r.subject.reference")).alias("patient_id"),
                    ref_id(F.col("r.encounter.reference")).alias("encounter_id"),
                    F.to_timestamp("r.onsetDateTime").alias("onset_ts"),
                    F.to_timestamp("r.recordedDate").alias("recorded_ts")],
   "known": ["clinicalStatus", "verificationStatus", "code", "subject", "encounter", "onsetDateTime", "recordedDate"]},
}
if resource_name not in SPECS:
    raise RuntimeError(f"No Silver mapping for {resource_name}. Add an entry to SPECS.")
spec = SPECS[resource_name]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_silver():
    known = spec["known"] + ["resourceType", "id", "meta", "text"]
    extra = F.to_json(F.map_filter(F.from_json("raw_json", "map<string,string>"),
                                   lambda k, v: ~F.array_contains(F.array(*[F.lit(x) for x in known]), k)))
    parsed = bronze.withColumn("r", F.from_json("raw_json", spec["ddl"]))
    flat = parsed.select(F.col("id"), *spec["cols"](), F.col("lastUpdated"), F.col("ingest_ts"),
                         F.col("run_id"), extra.alias("_extra_json"))
    w = Window.partitionBy("id").orderBy(F.col("lastUpdated").desc(), F.col("ingest_ts").desc())
    src = flat.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    if part_col == "lastUpdated_year_month":
        src = src.withColumn(part_col, F.date_format("lastUpdated", "yyyy-MM"))
    return src

def transform_and_merge():
    src = build_silver().cache()
    n_bronze = bronze.count()
    n_src    = src.count()
    if n_src == 0:
        log.info("Nothing to merge for run %s", run_id)
        src.unpersist()
        return {"read": n_bronze, "written": 0, "max_lu": None}

    if not DeltaTable.isDeltaTable(spark, silver_path):
        w = src.write.format("delta")
        (w.partitionBy(part_col) if part_col else w).save(silver_path)
        log.info("Created %s with %d rows", cfg["silver_table"], n_src)
    else:
        (DeltaTable.forPath(spark, silver_path).alias("t")
            .merge(src.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll(condition="s.lastUpdated > t.lastUpdated")
            .whenNotMatchedInsertAll()
            .execute())
        log.info("Merged %d rows into %s", n_src, cfg["silver_table"])

    # newest change in this run: from the Bronze notebook if given, else computed here
    max_lu = parse_fhir_ts(max_lastUpdated) if max_lastUpdated else src.agg(F.max("lastUpdated")).first()[0]
    src.unpersist()

    # advance the watermark ONLY now, after the MERGE succeeded
    current = get_watermark(cfg["config_id"])
    if max_lu and max_lu > current:
        set_watermark(cfg["config_id"], max_lu, run_id)
        log.info("Watermark moved %s -> %s", current, max_lu)
    else:
        log.info("Watermark unchanged (%s)", current)
    return {"read": n_bronze, "written": n_src, "max_lu": max_lu}

result = run_step("SILVER", cfg, run_id, pipeline_run_id, transform_and_merge)
log.info("Silver done: %s", {k: str(v) for k, v in result.items()})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Checks
silver = spark.read.format("delta").load(silver_path)
dups = silver.groupBy("id").count().where("count > 1").count()
print(f"{cfg['silver_table']}: {silver.count()} rows, {dups} duplicate ids")
assert dups == 0, "Duplicate ids found in Silver"
display(silver.orderBy(F.col("lastUpdated").desc()).limit(10))
display(latest_watermarks().where(F.col("config_id") == cfg["config_id"]))

notebookutils.notebook.exit(json.dumps({
    "resource": resource_name, "run_id": run_id,
    "rows_merged": result["written"],
    "max_lastUpdated": to_fhir_param(result["max_lu"]) if result["max_lu"] else None,
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
