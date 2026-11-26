# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # NB_Common_Utils (pure Spark)
# Shared helpers. Call it first with `%run NB_Common_Utils`.
# 
# No ODBC, no cursors, no connection strings: the warehouse is read and written with the **Fabric Spark connector** (`spark.read.synapsesql` / `df.write.synapsesql`).
# 
# The connector can read and **append**, but it cannot UPDATE or DELETE. So the control tables are used **append-only**:
# - `ctl_watermark`: a new row is appended on every successful run; the **latest `updated_at` row per `config_id` is the current watermark** (history is kept for free).
# - `ctl_run_log`, `ctl_schema_change_log`: append-only by nature.
# - `ctl_resource_config`: new resources are appended; existing rows are edited with a normal SQL query when you want to change a setting.

# CELL ********************

import json, time, uuid, random, logging, requests
from datetime import datetime, timezone
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType,
                               TimestampType, BooleanType)

spark.conf.set("spark.sql.session.timeZone", "UTC")     # all timestamps are handled as UTC

# ---- settings ----
WAREHOUSE_NAME = "WH_Metadata"
HEADERS        = {"Accept": "application/fhir+json"}
TIMEOUT        = (10, 90)             # connect, read (seconds)
RETRY_STATUS   = {429, 500, 502, 503, 504}

# ---- logging ----
log = logging.getLogger("fhir")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)
log.propagate = False

# ---- time helpers (naive UTC everywhere) ----
def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def parse_fhir_ts(s):
    """FHIR instant (Z or +hh:mm) -> naive UTC datetime, or None."""
    if not s:
        return None
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is not None:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)
    return d

def to_fhir_param(dt):
    """naive UTC datetime -> FHIR search value, e.g. 2026-11-25T05:49:17.579Z"""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# CELL ********************

# ---- warehouse access via the Spark connector ----
def read_meta(table):
    """Read dbo.<table> from WH_Metadata as a Spark DataFrame."""
    return spark.read.synapsesql(f"{WAREHOUSE_NAME}.dbo.{table}")

def write_meta(df, table):
    """Append a DataFrame to dbo.<table> in WH_Metadata."""
    df.write.mode("append").synapsesql(f"{WAREHOUSE_NAME}.dbo.{table}")

WATERMARK_SCHEMA = StructType([
    StructField("config_id",           IntegerType(),   False),
    StructField("last_watermark",      TimestampType(), True),
    StructField("last_success_run_id", StringType(),    True),
    StructField("updated_at",          TimestampType(), True),
])
RUNLOG_SCHEMA = StructType([
    StructField("run_id",          StringType(),    True),
    StructField("pipeline_run_id", StringType(),    True),
    StructField("config_id",       IntegerType(),   True),
    StructField("layer",           StringType(),    True),
    StructField("status",          StringType(),    True),
    StructField("rows_read",       LongType(),      True),
    StructField("rows_written",    LongType(),      True),
    StructField("start_time",      TimestampType(), True),
    StructField("end_time",        TimestampType(), True),
    StructField("error_message",   StringType(),    True),
])

def latest_watermarks():
    """Current watermark per config_id = the row with the newest updated_at."""
    w = Window.partitionBy("config_id").orderBy(F.col("updated_at").desc())
    return read_meta("ctl_watermark").withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

# CELL ********************

# ---- API access ----
class RetryableHTTP(Exception):
    pass

def call_api(url, params=None, max_attempts=5, backoff=2):
    """GET with timeout, retry and exponential backoff on 429/5xx, connection errors and timeouts."""
    last = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code in RETRY_STATUS:
                raise RetryableHTTP(f"HTTP {r.status_code} from {r.url}")
            r.raise_for_status()                 # other 4xx are not retried
            return r.json()
        except (RetryableHTTP, requests.ConnectionError, requests.Timeout) as e:
            last = e
            if attempt == max_attempts:
                break
            wait = min(60, backoff ** attempt) + random.random()
            log.warning("Attempt %d/%d failed (%s). Retrying in %.1fs", attempt, max_attempts, e, wait)
            time.sleep(wait)
    raise last

def paginate(url, params=None, max_pages=None):
    """Yield (page_number, bundle), following Bundle.link[relation='next'].
    Params go on page 1 only: the next link already carries them."""
    page = 0
    while url:
        bundle = call_api(url, params if page == 0 else None)
        page += 1
        yield page, bundle
        if max_pages and page >= max_pages:
            log.info("Stopped at max_pages=%d", max_pages)
            break
        url = next((l["url"] for l in bundle.get("link", []) if l.get("relation") == "next"), None)

# CELL ********************

# ---- metadata readers / writers (Spark only) ----
def get_config(resource_name):
    c = read_meta("ctl_resource_config").where((F.col("resource_name") == resource_name) & (F.col("is_active") == True))
    s = (read_meta("ctl_api_source").where(F.col("is_active") == True)
         .select("source_id", "base_url", "page_size"))
    row = c.join(s, "source_id").first()
    if row is None:
        raise RuntimeError(f"No active config for resource '{resource_name}'. Run NB_01_Load_API_Metadata.")
    return row.asDict()

def get_watermark(config_id):
    row = latest_watermarks().where(F.col("config_id") == config_id).first()
    if row is None or row["last_watermark"] is None:
        raise RuntimeError(f"No watermark for config_id={config_id}. Run NB_01_Load_API_Metadata.")
    return row["last_watermark"]

def set_watermark(config_id, ts, run_id):
    """Append a new watermark row; the newest row wins."""
    write_meta(spark.createDataFrame([(int(config_id), ts, run_id, utc_now())], WATERMARK_SCHEMA), "ctl_watermark")

def log_run(run_id, pipeline_run_id, config_id, layer, status,
            rows_read=0, rows_written=0, start=None, end=None, err=None):
    row = (run_id, pipeline_run_id, int(config_id), layer, status,
           int(rows_read), int(rows_written), start, end, ((err or "")[:4000] or None))
    write_meta(spark.createDataFrame([row], RUNLOG_SCHEMA), "ctl_run_log")

def run_step(layer, config, run_id, pipeline_run_id, fn):
    """Run fn(); log SUCCESS with row counts, or log FAILED and re-raise so the pipeline activity fails."""
    start = utc_now()
    try:
        result = fn()
        log_run(run_id, pipeline_run_id, config["config_id"], layer, "SUCCESS",
                result.get("read", 0), result.get("written", 0), start, utc_now())
        return result
    except Exception as e:
        log.exception("Step %s failed", layer)
        try:
            log_run(run_id, pipeline_run_id, config["config_id"], layer, "FAILED",
                    start=start, end=utc_now(), err=str(e))
        except Exception:
            log.exception("Could not write the FAILED row to ctl_run_log")
        raise

log.info("NB_Common_Utils loaded")

# MARKDOWN ********************

# ## Quick test (optional)
# Uncomment and run to check the helpers.

# CELL ********************

# pages = [len(b.get("entry", [])) for _, b in paginate("https://hapi.fhir.org/baseR4/Patient", {"_count": 5}, max_pages=2)]
# print("entries per page:", pages)
# c = get_config("Patient"); print(c["silver_table"], get_watermark(c["config_id"]))
