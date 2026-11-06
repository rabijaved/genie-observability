# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Observability — 01 Harvest (Genie REST API → Delta)
# MAGIC
# MAGIC Harvests everything the Genie (Genie Agents / Genie Spaces) REST API exposes into Unity Catalog Delta tables.
# MAGIC Step `02_build_gold` then joins this with system tables (audit, query history, billing).
# MAGIC
# MAGIC | Table | Source API | Notes |
# MAGIC |---|---|---|
# MAGIC | `genie_spaces` | `GET /genie/spaces`, `GET /genie/spaces/{id}?include_serialized_space=true` | metadata **+ configuration maturity** (tables, instructions, example SQL, join specs, benchmarks, sample questions) |
# MAGIC | `genie_space_tables` | serialized space | one row per table attached to a space |
# MAGIC | `genie_space_acl` | `GET /permissions/genie/{id}` | who can use / edit each space → reach vs. adoption |
# MAGIC | `genie_conversations` | `GET /conversations?include_all=true` | every user's conversations (needs CAN MANAGE) |
# MAGIC | `genie_messages` | `GET /conversations/{id}/messages` | question, status, latency, SQL, statement IDs, errors, feedback, thoughts, data sources, agent mode |
# MAGIC | `genie_comments` | `GET /conversations/{id}/list-comments` *(Public Preview)* | review requests / comments |
# MAGIC | `genie_eval_runs`, `genie_eval_results` | `GET /eval-runs`, `/results/{id}` *(Beta)* | benchmark accuracy over time |
# MAGIC | `genie_users` | SCIM users / service principals | numeric user_id → email |
# MAGIC
# MAGIC **Run as** a workspace admin or a service principal that has **CAN MANAGE** on every space you want covered.
# MAGIC Spaces where the caller only has CAN RUN/VIEW fall back to the caller's own conversations (flagged in `genie_spaces.harvest_scope`).
# MAGIC
# MAGIC **Incremental**: the conversation list is re-read every run (cheap); messages are only re-fetched for conversations that are new,
# MAGIC were created in the last `lookback_days`, or still had in-flight messages. Set `full_refresh=true` to re-read everything.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "genie_observability", "Schema")
dbutils.widgets.text("space_ids", "", "Space IDs (comma-separated, blank = all)")
dbutils.widgets.text("lookback_days", "7", "Re-fetch messages for conversations created in last N days")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")
dbutils.widgets.dropdown("harvest_comments", "true", ["true", "false"], "Harvest comments (Public Preview API)")
dbutils.widgets.dropdown("harvest_evals", "true", ["true", "false"], "Harvest benchmark evals (Beta API)")
dbutils.widgets.text("max_workers", "8", "Parallel API workers")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
SPACE_FILTER = {s.strip() for s in dbutils.widgets.get("space_ids").split(",") if s.strip()}
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days") or 7)
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"
HARVEST_COMMENTS = dbutils.widgets.get("harvest_comments") == "true"
HARVEST_EVALS = dbutils.widgets.get("harvest_evals") == "true"
MAX_WORKERS = int(dbutils.widgets.get("max_workers") or 8)
FQ = f"`{CATALOG}`.`{SCHEMA}`"

# COMMAND ----------

import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound, PermissionDenied
from pyspark.sql import types as T

w = WorkspaceClient()
WORKSPACE_ID = str(w.get_workspace_id())
WORKSPACE_URL = w.config.host
RUN_TS = datetime.now(timezone.utc)
print(f"Workspace {WORKSPACE_ID} ({WORKSPACE_URL}) → {CATALOG}.{SCHEMA}  full_refresh={FULL_REFRESH}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'Genie observability: API harvest + gold tables'")


class TransientError(Exception):
    """API kept failing with a retryable error (429/5xx/timeout). Callers must NOT treat this as 'no data'."""


def api(path, query=None, retries=8):
    """GET with exponential backoff (the SDK also retries 429/503 internally).
    Returns dict; None only for 403/404 (no access / gone); raises TransientError when retries are exhausted."""
    for attempt in range(retries):
        try:
            return w.api_client.do("GET", path, query=query or {})
        except (NotFound, PermissionDenied) as e:
            print(f"  [skip] {path}: {type(e).__name__} {str(e)[:120]}")
            return None
        except Exception as e:  # DatabricksError, timeouts, connection resets
            msg = str(e)
            retryable = not isinstance(e, DatabricksError) or any(
                k in msg for k in ("RATE_LIMIT", "429", "TEMPORARILY_UNAVAILABLE", "503", "502", "504", "INTERNAL_ERROR", "timed out"))
            if not retryable:
                print(f"  [error] {path}: {msg[:200]}")
                return None
            if attempt == retries - 1:
                raise TransientError(f"{path}: {msg[:200]}") from e
            time.sleep(min(120, 2 ** attempt + random.random() * 2))


def paged_list(path, key, query=None, page_size=100):
    """All pages of a list endpoint. Returns None if the endpoint is not accessible (403/404 on the first page).
    A transient failure on ANY page raises TransientError, so partial lists are never returned."""
    q = dict(query or {})
    q["page_size"] = page_size
    out = []
    while True:
        body = api(path, q)
        if body is None:
            if "page_token" in q:
                raise TransientError(f"{path}: page fetch returned 403/404 mid-pagination")
            return None
        out.extend(body.get(key, []) or [])
        tok = body.get("next_page_token")
        if not tok:
            return out
        q["page_token"] = tok


def ts(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc) if ms else None


def iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def pmap(fn, items):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        return list(ex.map(fn, items))


def table_exists(name):
    return spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.{name}")

# COMMAND ----------

# MAGIC %md ## 1. Spaces + configuration + ACLs

# COMMAND ----------

spaces = paged_list("/api/2.0/genie/spaces", "spaces")
if spaces is None:
    raise RuntimeError("Cannot list Genie spaces (403/404). Aborting before any table is modified.")
if SPACE_FILTER:
    spaces = [s for s in spaces if s["space_id"] in SPACE_FILTER]
print(f"{len(spaces)} spaces visible to {w.current_user.me().user_name}")


def _len(x):
    return len(x) if isinstance(x, list) else 0


def space_detail(s):
    """Returns (space_row | None, table_rows | None, acl_rows | None). None = fetch failed → keep previous rows."""
    try:
        return _space_detail(s)
    except TransientError as e:
        print(f"  [transient] space {s['space_id']}: {e} — keeping previous config/ACL rows")
        return None, None, None


def _space_detail(s):
    sid = s["space_id"]
    d = api(f"/api/2.0/genie/spaces/{sid}", {"include_serialized_space": "true"})
    if d is None:
        print(f"  [skip] space {sid}: detail not accessible — keeping previous config")
        return None, None, None
    cfg = {}
    try:
        cfg = json.loads(d.get("serialized_space") or "{}")
    except Exception:
        pass
    ds = cfg.get("data_sources", {}) or {}
    ins = cfg.get("instructions", {}) or {}
    tables = ds.get("tables", []) or []
    col_cfgs = [c for t in tables for c in (t.get("column_configs") or [])]
    text_ins = ins.get("text_instructions", []) or []
    instr_chars = sum(len("".join(t.get("content") or [])) for t in text_ins)
    acl = api(f"/api/2.0/permissions/genie/{sid}")
    acl_rows = None if acl is None else []          # None = no permission to read ACL → keep previous snapshot
    for a in (acl or {}).get("access_control_list", []) or []:
        if a.get("user_name"):
            ptype, pname = "USER", a["user_name"]
        elif a.get("group_name"):
            ptype, pname = "GROUP", a["group_name"]
        else:
            ptype, pname = "SERVICE_PRINCIPAL", a.get("service_principal_name")
        for p in a.get("all_permissions", []) or []:
            acl_rows.append((WORKSPACE_ID, sid, ptype, pname, a.get("display_name"), p.get("permission_level"), bool(p.get("inherited")), RUN_TS))
    row = dict(
        workspace_id=WORKSPACE_ID, space_id=sid, title=d.get("title") or s.get("title"),
        description=d.get("description") or s.get("description"),
        warehouse_id=d.get("warehouse_id") or s.get("warehouse_id"), parent_path=d.get("parent_path"),
        created_ts=iso(s.get("create_time") or d.get("create_time")), updated_ts=iso(s.get("update_time") or d.get("update_time")),
        n_tables=len(tables), n_column_configs=len(col_cfgs),
        n_entity_matching_columns=sum(1 for c in col_cfgs if c.get("enable_entity_matching")),
        n_sample_questions=_len((cfg.get("config") or {}).get("sample_questions")),
        n_text_instructions=len(text_ins), instruction_chars=instr_chars,
        n_example_sqls=_len(ins.get("example_question_sqls")), n_sql_functions=_len(ins.get("sql_functions")),
        n_join_specs=_len(ins.get("join_specs")), n_sql_snippets=_len((ins.get("sql_snippets") or {}).get("measures")) + _len((ins.get("sql_snippets") or {}).get("filters")) + _len((ins.get("sql_snippets") or {}).get("expressions")),
        n_benchmark_questions=_len((cfg.get("benchmarks") or {}).get("questions")),
        serialized_space=d.get("serialized_space"), harvested_at=RUN_TS,
    )
    table_rows = [(WORKSPACE_ID, sid, t.get("identifier"), _len(t.get("column_configs")), RUN_TS) for t in tables]
    return row, table_rows, acl_rows


details = pmap(space_detail, spaces)
space_rows = [d[0] for d in details if d[0] is not None]
# snapshot tables are only replaced for spaces whose fetch succeeded
tables_ok_ids = {s["space_id"] for s, d in zip(spaces, details) if d[1] is not None}
acl_ok_ids = {s["space_id"] for s, d in zip(spaces, details) if d[2] is not None}
space_table_rows = [r for d in details if d[1] is not None for r in d[1]]
acl_rows = [r for d in details if d[2] is not None for r in d[2]]
n_space_failures = len(spaces) - len(space_rows)
print(f"space details: {len(space_rows)} ok, {n_space_failures} kept from previous run")

# COMMAND ----------

# MAGIC %md ## 2. Conversations (all users)

# COMMAND ----------

known = {}
if table_exists("genie_messages") and not FULL_REFRESH:
    for r in spark.sql(f"""
        SELECT conversation_id, MAX(created_ts) AS last_msg,
               MAX(CASE WHEN status NOT IN ('COMPLETED','FAILED','CANCELLED','QUERY_RESULT_EXPIRED') THEN 1 ELSE 0 END) AS inflight
        FROM {FQ}.genie_messages WHERE workspace_id = '{WORKSPACE_ID}' GROUP BY 1""").collect():
        known[r.conversation_id] = r
cutoff = RUN_TS - timedelta(days=LOOKBACK_DAYS)


def list_convs(s):
    sid = s["space_id"]
    try:
        convs = paged_list(f"/api/2.0/genie/spaces/{sid}/conversations", "conversations", {"include_all": "true"})
        scope = "ALL_USERS"
        if convs is None:  # 403: caller lacks CAN MANAGE → own conversations only
            convs = paged_list(f"/api/2.0/genie/spaces/{sid}/conversations", "conversations") or []
            scope = "CALLER_ONLY"
        return sid, scope, convs
    except TransientError as e:
        print(f"  [transient] conversations for space {sid}: {e} — skipped this run")
        return sid, None, []


conv_lists = pmap(list_convs, spaces)
prev_scope = {}
if table_exists("genie_spaces"):
    prev_scope = {r.space_id: (r.harvest_scope, r.n_conversations_listed) for r in spark.sql(
        f"SELECT space_id, harvest_scope, n_conversations_listed FROM {FQ}.genie_spaces WHERE workspace_id = '{WORKSPACE_ID}'").collect()}
listed = {sid: (scope, len(c)) for sid, scope, c in conv_lists}
n_conv_list_failures = sum(1 for _, scope, _ in conv_lists if scope is None)
for r in space_rows:
    scope, n = listed.get(r["space_id"], (None, 0))
    if scope is None:  # listing failed this run → keep previous values
        scope, n = prev_scope.get(r["space_id"], ("NONE", 0))
    r["harvest_scope"], r["n_conversations_listed"] = scope, n

conv_rows, to_fetch = [], []
for sid, scope, convs in conv_lists:
    if scope is None:
        continue
    for c in convs:
        cid = c.get("conversation_id")
        created = ts(c.get("created_timestamp"))
        conv_rows.append(dict(workspace_id=WORKSPACE_ID, space_id=sid, conversation_id=cid, title=c.get("title"),
                              agent_type=c.get("agent_type"), created_ts=created,
                              last_updated_ts=ts(c.get("last_updated_timestamp")), harvested_at=RUN_TS))
        k = known.get(cid)
        if FULL_REFRESH or k is None or (created and created >= cutoff) or (k and k.inflight):
            to_fetch.append((sid, cid, c.get("agent_type")))
print(f"{len(conv_rows)} conversations listed; fetching messages for {len(to_fetch)}")

# COMMAND ----------

# MAGIC %md ## 3. Messages (+ comments)

# COMMAND ----------

def parse_message(sid, cid, agent_type, idx, m):
    atts = m.get("attachments") or []
    q_atts = [a["query"] for a in atts if a.get("query")]
    t_atts = [a["text"] for a in atts if a.get("text")]
    sq = next((a["suggested_questions"] for a in atts if a.get("suggested_questions")), None)
    q = q_atts[0] if q_atts else {}
    meta = q.get("query_result_metadata") or {}
    thoughts = [th for qa in q_atts for th in (qa.get("thoughts") or [])]
    data_sources = sorted({ln.strip("- ").strip() for th in thoughts if th.get("thought_type") == "THOUGHT_TYPE_DATA_SOURCING"
                           for ln in (th.get("content") or "").splitlines() if ln.strip("- ").strip()})
    stmt_ids = [qa.get("statement_id") for qa in q_atts if qa.get("statement_id")]
    if (m.get("query_result") or {}).get("statement_id"):
        stmt_ids.append(m["query_result"]["statement_id"])
    answer = next((t for t in t_atts if t.get("purpose") != "TEXT_ATTACHMENT_PURPOSE_FOLLOW_UP"), t_atts[0] if t_atts else {})
    err = m.get("error") or {}
    fb = m.get("feedback") or {}
    created, updated = m.get("created_timestamp"), m.get("last_updated_timestamp")
    return dict(
        workspace_id=WORKSPACE_ID, space_id=sid, conversation_id=cid, message_id=m.get("message_id") or m.get("id"),
        user_id=str(m.get("user_id")) if m.get("user_id") is not None else None, agent_type=agent_type,
        question=m.get("content"), status=m.get("status"),
        created_ts=ts(created), last_updated_ts=ts(updated),
        response_time_sec=(updated - created) / 1000.0 if created and updated else None,
        message_position=idx + 1, auto_regenerate_count=m.get("auto_regenerate_count"),
        attachment_count=len(atts), n_query_attachments=len(q_atts),
        has_text=bool(t_atts), has_viz=any(a.get("viz") for a in atts), has_suggested_questions=bool(sq),
        sql=q.get("query"), sql_title=q.get("title"), sql_description=q.get("description"),
        statement_ids=sorted(set(stmt_ids)), row_count=meta.get("row_count"), is_truncated=meta.get("is_truncated"),
        text_response=answer.get("content"), text_purpose=answer.get("purpose"),
        is_clarification=any("CLARIF" in (t.get("purpose") or "") for t in t_atts),
        suggested_questions=(sq or {}).get("questions") or [],
        error_type=err.get("type"), error_message=err.get("error"),
        feedback_rating=fb.get("rating"),
        thought_count=len(thoughts), data_sources=data_sources,
        raw_json=json.dumps(m), harvested_at=RUN_TS,
    )


def fetch_conv(args):
    try:
        return _fetch_conv(args)
    except TransientError as e:
        print(f"  [transient] conversation {args[1]}: {e}")
        return None


def _fetch_conv(args):
    sid, cid, agent_type = args
    msgs = paged_list(f"/api/2.0/genie/spaces/{sid}/conversations/{cid}/messages", "messages") or []
    msgs.sort(key=lambda m: m.get("created_timestamp") or 0)
    rows = [parse_message(sid, cid, agent_type, i, m) for i, m in enumerate(msgs)]
    comments = []
    if HARVEST_COMMENTS and msgs:
        for c in paged_list(f"/api/2.0/genie/spaces/{sid}/conversations/{cid}/list-comments", "comments") or []:
            comments.append((WORKSPACE_ID, sid, cid, c.get("message_id"), c.get("comment_id") or c.get("id"),
                             str(c.get("user_id") or c.get("created_by") or ""), c.get("comment_type") or c.get("type"),
                             c.get("content") or c.get("text"), ts(c.get("created_timestamp")), json.dumps(c), RUN_TS))
    return rows, comments


results = pmap(fetch_conv, to_fetch)
failed_convs = [a[1] for a, r in zip(to_fetch, results) if r is None]
results = [r for r in results if r is not None]
msg_rows = [r for rs, _ in results for r in rs]
comment_rows = [c for _, cs in results for c in cs]
if failed_convs:
    print(f"{len(failed_convs)} conversations failed transiently; they will be retried next run")
print(f"{len(msg_rows)} messages, {len(comment_rows)} comments")

# conversation starter = user of first message
first_user = {}
for r in sorted(msg_rows, key=lambda r: r["message_position"]):
    first_user.setdefault(r["conversation_id"], r["user_id"])
for c in conv_rows:
    c["user_id"] = first_user.get(c["conversation_id"])  # None when not re-fetched; MERGE keeps the stored value

# COMMAND ----------

# MAGIC %md ## 4. Benchmark evaluations (Beta API)

# COMMAND ----------

eval_run_rows, eval_result_rows = [], []
if HARVEST_EVALS:
    done_runs = set()
    if table_exists("genie_eval_results") and not FULL_REFRESH:
        done_runs = {r.eval_run_id for r in spark.sql(f"SELECT DISTINCT eval_run_id FROM {FQ}.genie_eval_results").collect()}

    def fetch_evals(s):
        try:
            return _fetch_evals(s)
        except TransientError as e:
            print(f"  [transient] evals for space {s['space_id']}: {e}")
            return [], []

    def _fetch_evals(s):
        sid = s["space_id"]
        runs = paged_list(f"/api/2.0/genie/spaces/{sid}/eval-runs", "eval_runs") or []
        rr, res = [], []
        for r in runs:
            nq, nc = r.get("num_questions") or 0, r.get("num_correct") or 0
            rr.append((WORKSPACE_ID, sid, r["eval_run_id"], r.get("eval_run_status"), str(r.get("run_by_user") or ""),
                       ts(r.get("created_timestamp")), ts(r.get("last_updated_timestamp")), nq, nc,
                       r.get("num_needs_review") or 0, (nc / nq) if nq else None, RUN_TS))
            if r.get("eval_run_status") == "DONE" and r["eval_run_id"] not in done_runs:
                for e in paged_list(f"/api/2.0/genie/spaces/{sid}/eval-runs/{r['eval_run_id']}/results", "eval_results") or []:
                    d = api(f"/api/2.0/genie/spaces/{sid}/eval-runs/{r['eval_run_id']}/results/{e['result_id']}") or {}
                    actual_sql = next((x.get("response") for x in d.get("actual_response", []) or [] if x.get("response_type") == "SQL"), None)
                    res.append((WORKSPACE_ID, sid, r["eval_run_id"], e["result_id"], e.get("benchmark_question_id"),
                                e.get("question"), d.get("assessment"), bool(d.get("manual_assessment")),
                                e.get("benchmark_answer"), actual_sql, json.dumps(d.get("assessment_reasons") or []), RUN_TS))
        return rr, res

    for rr, res in pmap(fetch_evals, spaces):
        eval_run_rows += rr
        eval_result_rows += res
print(f"{len(eval_run_rows)} eval runs, {len(eval_result_rows)} new eval results")

# COMMAND ----------

# MAGIC %md ## 5. Resolve user IDs → email

# COMMAND ----------

ids = {r["user_id"] for r in msg_rows if r["user_id"]} | {r[4] for r in eval_run_rows if r[4]} | {c[5] for c in comment_rows if c[5]}
if table_exists("genie_users"):
    ids -= {r.user_id for r in spark.sql(f"SELECT user_id FROM {FQ}.genie_users WHERE user_name IS NOT NULL").collect()}


def resolve(uid):
    try:
        u = w.users.get(uid)
        return (uid, u.user_name, u.display_name, bool(u.active), "USER", RUN_TS)
    except Exception:
        pass
    try:
        sp = w.service_principals.get(uid)
        return (uid, sp.application_id, sp.display_name, bool(sp.active), "SERVICE_PRINCIPAL", RUN_TS)
    except Exception:
        return (uid, None, None, None, "UNKNOWN", RUN_TS)


user_rows = pmap(resolve, sorted(ids))
print(f"resolved {len(user_rows)} new principals")

# COMMAND ----------

# MAGIC %md ## 6. Write to Delta (MERGE)

# COMMAND ----------

TS, S, L, I, B, D = T.TimestampType(), T.StringType(), T.LongType(), T.IntegerType(), T.BooleanType(), T.DoubleType()
AS = T.ArrayType(S)


def schema(*cols):
    return T.StructType([T.StructField(n, t) for n, t in cols])


SCHEMAS = {
    "genie_spaces": schema(("workspace_id", S), ("space_id", S), ("title", S), ("description", S), ("warehouse_id", S), ("parent_path", S),
                           ("created_ts", TS), ("updated_ts", TS), ("n_tables", I), ("n_column_configs", I), ("n_entity_matching_columns", I),
                           ("n_sample_questions", I), ("n_text_instructions", I), ("instruction_chars", I), ("n_example_sqls", I),
                           ("n_sql_functions", I), ("n_join_specs", I), ("n_sql_snippets", I), ("n_benchmark_questions", I),
                           ("harvest_scope", S), ("n_conversations_listed", I), ("serialized_space", S), ("harvested_at", TS)),
    "genie_space_tables": schema(("workspace_id", S), ("space_id", S), ("table_identifier", S), ("n_column_configs", I), ("harvested_at", TS)),
    "genie_space_acl": schema(("workspace_id", S), ("space_id", S), ("principal_type", S), ("principal", S), ("display_name", S),
                              ("permission_level", S), ("inherited", B), ("harvested_at", TS)),
    "genie_conversations": schema(("workspace_id", S), ("space_id", S), ("conversation_id", S), ("title", S), ("agent_type", S),
                                  ("user_id", S), ("created_ts", TS), ("last_updated_ts", TS), ("harvested_at", TS)),
    "genie_messages": schema(("workspace_id", S), ("space_id", S), ("conversation_id", S), ("message_id", S), ("user_id", S), ("agent_type", S),
                             ("question", S), ("status", S), ("created_ts", TS), ("last_updated_ts", TS), ("response_time_sec", D),
                             ("message_position", I), ("auto_regenerate_count", I), ("attachment_count", I), ("n_query_attachments", I),
                             ("has_text", B), ("has_viz", B), ("has_suggested_questions", B), ("sql", S), ("sql_title", S), ("sql_description", S),
                             ("statement_ids", AS), ("row_count", L), ("is_truncated", B), ("text_response", S), ("text_purpose", S),
                             ("is_clarification", B), ("suggested_questions", AS), ("error_type", S), ("error_message", S),
                             ("feedback_rating", S), ("thought_count", I), ("data_sources", AS), ("raw_json", S), ("harvested_at", TS)),
    "genie_comments": schema(("workspace_id", S), ("space_id", S), ("conversation_id", S), ("message_id", S), ("comment_id", S), ("user_id", S),
                             ("comment_type", S), ("content", S), ("created_ts", TS), ("raw_json", S), ("harvested_at", TS)),
    "genie_eval_runs": schema(("workspace_id", S), ("space_id", S), ("eval_run_id", S), ("status", S), ("run_by_user_id", S), ("created_ts", TS),
                              ("last_updated_ts", TS), ("num_questions", I), ("num_correct", I), ("num_needs_review", I), ("accuracy", D), ("harvested_at", TS)),
    "genie_eval_results": schema(("workspace_id", S), ("space_id", S), ("eval_run_id", S), ("result_id", S), ("benchmark_question_id", S),
                                 ("question", S), ("assessment", S), ("manual_assessment", B), ("expected_sql", S), ("actual_sql", S),
                                 ("assessment_reasons", S), ("harvested_at", TS)),
    "genie_users": schema(("user_id", S), ("user_name", S), ("display_name", S), ("active", B), ("principal_type", S), ("harvested_at", TS)),
}
KEYS = {
    "genie_spaces": ["workspace_id", "space_id"],
    "genie_space_tables": None,  # snapshot per workspace
    "genie_space_acl": None,     # snapshot per workspace
    "genie_conversations": ["workspace_id", "conversation_id"],
    "genie_messages": ["workspace_id", "message_id"],
    "genie_comments": ["workspace_id", "conversation_id", "comment_id"],
    "genie_eval_runs": ["workspace_id", "eval_run_id"],
    "genie_eval_results": ["workspace_id", "result_id"],
    "genie_users": ["user_id"],
}

for name, sch in SCHEMAS.items():
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in sch.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {FQ}.{name} ({cols}) CLUSTER BY AUTO")


# columns that must not be overwritten with NULL when a run doesn't have the value
PRESERVE = {"genie_conversations": ["user_id", "created_ts", "last_updated_ts"]}


def upsert(name, rows, snapshot_space_ids=None):
    sch = SCHEMAS[name]
    if rows and isinstance(rows[0], dict):
        rows = [tuple(r.get(f.name) for f in sch.fields) for r in rows]
    keys = KEYS[name]
    if keys is None:  # snapshot replace, only for spaces fetched successfully this run (atomic: one Delta transaction)
        if not snapshot_space_ids:
            print(f"{name}: no successfully fetched spaces — previous snapshot kept")
            return
        ids = ",".join(f"'{x}'" for x in sorted(snapshot_space_ids))
        spark.createDataFrame(rows or [], sch).write.mode("overwrite") \
            .option("replaceWhere", f"workspace_id = '{WORKSPACE_ID}' AND space_id IN ({ids})") \
            .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
        print(f"{name}: replaced {len(snapshot_space_ids)} spaces with {len(rows)} rows")
        return
    if not rows:
        print(f"{name}: nothing to merge")
        return
    view = f"_in_{name}"
    spark.createDataFrame(rows, sch).dropDuplicates(keys).createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    keep = set(PRESERVE.get(name, []))
    sets = ", ".join(f"t.`{f.name}` = " + (f"COALESCE(s.`{f.name}`, t.`{f.name}`)" if f.name in keep else f"s.`{f.name}`") for f in sch.fields)
    spark.sql(f"MERGE INTO {FQ}.{name} t USING {view} s ON {on} WHEN MATCHED THEN UPDATE SET {sets} WHEN NOT MATCHED THEN INSERT *")
    print(f"{name}: merged {len(rows)} rows")


upsert("genie_spaces", space_rows)
upsert("genie_space_tables", space_table_rows, snapshot_space_ids=tables_ok_ids)
upsert("genie_space_acl", acl_rows, snapshot_space_ids=acl_ok_ids)
upsert("genie_conversations", conv_rows)
upsert("genie_messages", msg_rows)
upsert("genie_comments", comment_rows)
upsert("genie_eval_runs", eval_run_rows)
upsert("genie_eval_results", eval_result_rows)
upsert("genie_users", user_rows)

# Backfill conversation starters from harvested messages (repairs rows written before the PRESERVE fix)
spark.sql(f"""
MERGE INTO {FQ}.genie_conversations t
USING (SELECT workspace_id, conversation_id, MIN_BY(user_id, message_position) AS user_id
       FROM {FQ}.genie_messages WHERE user_id IS NOT NULL GROUP BY 1, 2) s
ON t.workspace_id = s.workspace_id AND t.conversation_id = s.conversation_id
WHEN MATCHED AND t.user_id IS NULL THEN UPDATE SET t.user_id = s.user_id""")

# Run log. Spaces no longer returned by the API (trashed / access removed) are kept in genie_spaces;
# gold marks a space active only if it appears in the latest unfiltered run's live_space_ids.
spark.sql(f"CREATE TABLE IF NOT EXISTS {FQ}.genie_harvest_log (workspace_id STRING, run_ts TIMESTAMP, n_spaces INT, n_conversations INT, "
          f"n_messages_fetched INT, n_comments INT, n_eval_runs INT, live_space_ids ARRAY<STRING>, full_refresh BOOLEAN, space_filter BOOLEAN)")
for col in ("n_failures",):
    if col not in spark.table(f"{CATALOG}.{SCHEMA}.genie_harvest_log").columns:
        spark.sql(f"ALTER TABLE {FQ}.genie_harvest_log ADD COLUMNS ({col} INT)")
n_failures = n_space_failures + n_conv_list_failures + len(failed_convs)
# live_space_ids = every space the list API returned (the list call itself must have succeeded to get here)
spark.createDataFrame([(WORKSPACE_ID, RUN_TS, len(spaces), len(conv_rows), len(msg_rows), len(comment_rows), len(eval_run_rows),
                        [s["space_id"] for s in spaces], FULL_REFRESH, bool(SPACE_FILTER), n_failures)],
                      spark.table(f"{CATALOG}.{SCHEMA}.genie_harvest_log").schema).write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.genie_harvest_log")

dbutils.jobs.taskValues.set("workspace_id", WORKSPACE_ID)
print(f"Harvest complete ({n_failures} transient failures).")
# Fail loudly if too much was skipped, AFTER everything that succeeded has been written.
if n_failures and n_failures > max(5, 0.05 * (len(spaces) + len(to_fetch))):
    raise RuntimeError(f"{n_failures} spaces/conversations failed transiently — data written is partial; re-run the job.")
