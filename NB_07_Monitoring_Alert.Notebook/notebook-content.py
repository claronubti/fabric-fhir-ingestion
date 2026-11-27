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

# # NB_07_Monitoring_Alert (pure Spark)
# Builds the run summary for one pipeline run from `ctl_run_log` and `ctl_schema_change_log` and **returns** it to the pipeline as JSON (`overall_status`, `subject`, `html`, ...). The pipeline's Outlook activity sends the email; this notebook sends nothing itself.
# 
# - `run_id` = the pipeline run id (the pipeline passes `@pipeline().RunId`). Blank = the most recent run in the log (handy for manual testing).
# - Status is `FAILED` if any step logged `FAILED`, if `critical_failures > 0`, or if the run logged nothing at all.
# - Gold is logged by the pipeline (layer `GOLD`, config_id 0).
# 
# Run order: `NB_Common_Utils` imported first.

# PARAMETERS CELL ********************

# PARAMETERS
run_id            = ""            # pipeline run id; blank = latest run in ctl_run_log
env               = "Dev"
pipeline_name     = "PL_FHIR_Master"
critical_failures = 0             # from the pipeline's critical-failure lookup


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

import html as _html
from datetime import date, timedelta

LAYER_ORDER = {"BRONZE": 1, "SCHEMA": 2, "SILVER": 3, "GOLD": 4}

runlog = read_meta("ctl_run_log")
if not run_id:
    latest = runlog.orderBy(F.col("start_time").desc()).select("pipeline_run_id").first()
    if latest is None:
        raise RuntimeError("ctl_run_log is empty; nothing to report.")
    run_id = latest["pipeline_run_id"]
log.info("Reporting on pipeline run %s", run_id)

cfg = read_meta("ctl_resource_config").select("config_id", "resource_name", "load_order")
steps = (runlog.where(F.col("pipeline_run_id") == run_id)
         .join(cfg, "config_id", "left")
         .select("resource_name", "load_order", "layer", "status", "rows_read", "rows_written",
                 "start_time", "end_time", "error_message")
         .collect())

def dur(r):
    return round((r["end_time"] - r["start_time"]).total_seconds(), 1) if r["start_time"] and r["end_time"] else None

rows = sorted(steps, key=lambda r: (r["load_order"] if r["load_order"] is not None else 99,
                                    LAYER_ORDER.get(r["layer"], 9)))
failed = [r for r in rows if r["status"] == "FAILED"]
if not rows:
    overall = "FAILED"            # a run that logged nothing is not a success
    log.warning("No log rows for pipeline run %s", run_id)
elif failed or int(critical_failures or 0) > 0:
    overall = "FAILED"
else:
    overall = "SUCCESS"

run_start = min((r["start_time"] for r in rows if r["start_time"]), default=None)
run_end   = max((r["end_time"] for r in rows if r["end_time"]), default=None)

# schema changes detected during this run's window
changes = []
if run_start:
    changes = (read_meta("ctl_schema_change_log")
               .where(F.col("detected_at") >= F.lit(run_start - timedelta(minutes=1)))
               .join(cfg, "config_id", "left")
               .orderBy("resource_name", "change_type")
               .select("resource_name", "column_path", "change_type", "old_type", "new_type", "action_taken")
               .limit(50).collect())

total_written = sum((r["rows_written"] or 0) for r in rows if r["layer"] == "SILVER")
log.info("overall=%s steps=%d failed=%d schema_changes=%d", overall, len(rows), len(failed), len(changes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

e = lambda x: _html.escape("" if x is None else str(x))
colour = "#1a7f37" if overall == "SUCCESS" else "#cf222e"
TH = "style='text-align:left;padding:6px 10px;background:#f0f2f5;border:1px solid #d0d7de'"
TD = "style='padding:6px 10px;border:1px solid #d0d7de'"

def row_html(r):
    bad = r["status"] == "FAILED"
    st = f"<b style='color:{'#cf222e' if bad else '#1a7f37'}'>{e(r['status'])}</b>"
    return (f"<tr><td {TD}>{e(r['resource_name'] or '(Gold layer)')}</td><td {TD}>{e(r['layer'])}</td><td {TD}>{st}</td>"
            f"<td {TD}>{e(r['rows_read'])}</td><td {TD}>{e(r['rows_written'])}</td><td {TD}>{e(dur(r))}</td></tr>")

steps_html = "".join(row_html(r) for r in rows) or f"<tr><td {TD} colspan=6>No steps were logged for this run.</td></tr>"

fail_html = ""
if failed:
    items = "".join(f"<li><b>{e(r['resource_name'] or 'Gold')} / {e(r['layer'])}</b>: {e((r['error_message'] or '')[:300])}</li>" for r in failed)
    fail_html = f"<h3 style='color:#cf222e'>Failures</h3><ul>{items}</ul>"

chg_html = ""
if changes:
    body = "".join(f"<tr><td {TD}>{e(c['resource_name'])}</td><td {TD}>{e(c['column_path'])}</td><td {TD}>{e(c['change_type'])}</td>"
                   f"<td {TD}>{e(c['old_type'])}</td><td {TD}>{e(c['new_type'])}</td><td {TD}>{e(c['action_taken'])}</td></tr>" for c in changes)
    chg_html = (f"<h3>Schema changes ({len(changes)})</h3><table style='border-collapse:collapse;font-size:13px'>"
                f"<tr><th {TH}>Resource</th><th {TH}>Path</th><th {TH}>Change</th><th {TH}>Old</th><th {TH}>New</th><th {TH}>Action</th></tr>{body}</table>")

html_body = f"""<div style='font-family:Segoe UI,Arial,sans-serif;font-size:14px'>
<h2 style='color:{colour};margin-bottom:4px'>{e(pipeline_name)} [{e(env)}]: {overall}</h2>
<p style='margin-top:0'>Run <code>{e(run_id)}</code><br>Started {e(run_start)} UTC, finished {e(run_end)} UTC<br>
Rows merged into Silver: <b>{total_written}</b> &nbsp;|&nbsp; Critical failures: <b>{int(critical_failures or 0)}</b></p>
{fail_html}
<h3>Steps</h3>
<table style='border-collapse:collapse;font-size:13px'>
<tr><th {TH}>Resource</th><th {TH}>Layer</th><th {TH}>Status</th><th {TH}>Rows read</th><th {TH}>Rows written</th><th {TH}>Seconds</th></tr>
{steps_html}</table>
{chg_html}
</div>"""

subject = f"[FHIR][{env}] {overall} - {date.today().isoformat()}"
payload = {"overall_status": overall, "subject": subject, "html": html_body,
           "failed_steps": len(failed), "schema_changes": len(changes), "rows_merged": total_written, "run_id": run_id}

# Preview in the notebook (works for manual runs)
displayHTML(html_body)
notebookutils.notebook.exit(json.dumps(payload))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
