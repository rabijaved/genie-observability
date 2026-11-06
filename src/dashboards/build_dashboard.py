"""Generates genie_observability.lvdash.json.  Run:  python src/dashboards/build_dashboard.py
All dataset queries use bare table names; catalog/schema are injected by the bundle (dataset_catalog / dataset_schema)."""
import json
import pathlib

OUT = pathlib.Path(__file__).with_name("genie_observability.lvdash.json")

# ----------------------------------------------------------------------------------------------- datasets
QUESTION_CATS = "('QUESTION','AGENT_MODE_QUESTION')"
DATASETS = {
    "ds_activity": ("Genie activity (audit log)", """
SELECT event_date, event_time, date_trunc('WEEK', event_date) AS event_week, hour(event_time) AS hour_of_day,
       date_format(event_time, 'E') AS day_of_week,
       COALESCE(space_title, CASE WHEN service_name = 'genieChat' THEN 'Genie One (no space)' ELSE concat('Unknown space ', space_id) END) AS space_title,
       user_email, event_category, channel, service_name, is_success, conversation_id, message_id, feedback_rating,
       CASE WHEN event_category IN ('QUESTION','AGENT_MODE_QUESTION') THEN 1 ELSE 0 END AS is_question,
       CASE WHEN event_category IN ('QUESTION','AGENT_MODE_QUESTION') THEN user_email END AS asking_user,
       CASE WHEN event_category IN ('QUESTION','AGENT_MODE_QUESTION') THEN space_title END AS asked_space,
       CASE WHEN event_category = 'GENIE_ONE_QUESTION' THEN 1 ELSE 0 END AS is_genie_one_question,
       CASE WHEN event_category = 'GENIE_ONE_QUESTION' THEN user_email END AS genie_one_user
FROM g_audit_events
""", None),
    "ds_messages": ("Genie questions (API harvest, enriched)", """
SELECT created_date, created_ts, date_trunc('WEEK', created_date) AS created_week, space_title, user_email, agent_type, outcome,
       CASE WHEN is_completed THEN 1 ELSE 0 END AS completed, response_time_sec, genie_reasoning_sec, sql_total_sec, sql_queue_sec,
       latency_bucket, CASE WHEN is_follow_up THEN 'Follow-up' ELSE 'New conversation' END AS turn_type,
       question, sql_title, sql, text_response, row_count, n_statements, error_type, error_message, feedback_rating,
       thumbs_up, thumbs_down, is_rated, n_review_requests, sql_cost_usd, hour_of_day, day_of_week, conversation_id, message_id,
       CASE WHEN outcome IN ('Failed','SQL error','No answer') THEN 1 ELSE 0 END AS is_bad_outcome
FROM g_messages
""", [
        {"displayName": "Completion rate", "description": "Messages with status COMPLETED / all messages", "expression": "AVG(`completed`)"},
        {"displayName": "Positive feedback rate", "description": "Thumbs up / (thumbs up + thumbs down)",
         "expression": "SUM(`thumbs_up`) / NULLIF(SUM(`thumbs_up`) + SUM(`thumbs_down`), 0)"},
        {"displayName": "Median response (s)", "description": "Median end-to-end response time", "expression": "PERCENTILE(`response_time_sec`, 0.5)"},
        {"displayName": "Feedback coverage", "description": "Share of messages that received a 👍 or 👎",
         "expression": "AVG(`is_rated`)"},
        {"displayName": "P90 response (s)", "description": "90th percentile end-to-end response time", "expression": "PERCENTILE(`response_time_sec`, 0.9)"},
    ]),
    "ds_feedback": ("Feedback (latest rating per rater per answer)", """
SELECT feedback_date, feedback_ts, surface, channel, rater_email, rating,
       CASE rating WHEN 'POSITIVE' THEN '👍 Positive' ELSE '👎 Negative' END AS rating_label,
       thumbs_up, thumbs_down, 1 AS ratings, n_feedback_events, CASE WHEN rating_changed THEN 1 ELSE 0 END AS rating_changed,
       space_title, conversation_id, message_id, asker_email,
       CASE WHEN rated_own_answer THEN 'Own question' WHEN rated_own_answer = false THEN 'Another user' ELSE 'Unknown' END AS rater_relation,
       question, COALESCE(outcome, 'Not harvested') AS outcome, response_time_sec, COALESCE(latency_bucket, 'Unknown') AS latency_bucket,
       COALESCE(turn_type, 'Unknown') AS turn_type, agent_type, row_count, sql, text_response,
       CASE WHEN has_review_request THEN 'Yes' ELSE 'No' END AS review_requested, minutes_to_feedback
FROM g_feedback
""", [
        {"displayName": "Positive rate", "description": "👍 ÷ (👍 + 👎)", "expression": "SUM(`thumbs_up`) / NULLIF(SUM(`ratings`), 0)"},
        {"displayName": "Median minutes to feedback", "description": "Median time from answer to first rating", "expression": "PERCENTILE(`minutes_to_feedback`, 0.5)"},
    ]),
    "ds_raters": ("Feedback by rater", """
SELECT rater_email, surface, COUNT(*) AS ratings, SUM(thumbs_up) AS thumbs_up, SUM(thumbs_down) AS thumbs_down,
       SUM(thumbs_up) / COUNT(*) AS positive_rate, SUM(CASE WHEN rating_changed THEN 1 ELSE 0 END) AS rating_changes,
       COUNT(DISTINCT space_title) AS spaces_rated, MIN(feedback_date) AS first_feedback, MAX(feedback_date) AS last_feedback
FROM g_feedback WHERE feedback_date BETWEEN :date_range.min AND :date_range.max
GROUP BY rater_email, surface ORDER BY ratings DESC
""", None),
    "ds_scorecard": ("Space scorecard (30d)", "SELECT * FROM g_space_scorecard ORDER BY questions_30d DESC, space_title", None),
    "ds_sql_cost": ("Genie SQL warehouse cost (daily)", """
SELECT query_date, COALESCE(space_title, concat('Deleted space ', left(space_id, 8))) AS space_title, executed_by, execution_status, warehouse_id,
       COUNT(*) AS statements, SUM(total_duration_ms) / 1000.0 AS total_duration_sec,
       SUM(attributed_cost_usd) AS warehouse_cost_usd, SUM(attributed_dbus) AS warehouse_dbus,
       SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_statements,
       SUM(CASE WHEN from_result_cache THEN 1 ELSE 0 END) AS cached_statements
FROM g_query_stats GROUP BY ALL
""", None),
    "ds_billing": ("Genie LLM consumption (billing)", """
SELECT usage_date, surface, channel, COALESCE(space_title, space_id, CONCAT('(', surface, ')')) AS space_title, run_as, sku_name,
       CASE WHEN is_free_tier THEN 'Free allowance' ELSE 'Billable' END AS billing_type, dbus, list_cost_usd, billable_list_cost_usd
FROM g_genie_billing
""", None),
    "ds_evals": ("Benchmark runs", "SELECT run_date, created_ts, space_title, accuracy, num_questions, num_correct, num_needs_review, run_by, is_latest FROM g_eval_runs ORDER BY created_ts", None),
    "ds_tables": ("Attached vs used tables", "SELECT space_title, table_identifier, n_column_configs, n_messages_using_90d, last_used_ts, harvest_scope, CASE WHEN is_unused_90d THEN 'Unused (90d)' WHEN is_unused_90d IS NULL THEN 'Unknown (partial harvest)' ELSE 'Used' END AS usage_status FROM g_table_usage ORDER BY space_title, n_messages_using_90d DESC", None),
    "ds_users": ("User summary", f"""
WITH q AS (SELECT * FROM g_audit_events WHERE event_category IN {QUESTION_CATS}
           AND event_date BETWEEN :date_range.min AND :date_range.max)
SELECT user_email, MIN(event_date) AS first_seen, MAX(event_date) AS last_seen, COUNT(*) AS questions,
       COUNT(DISTINCT event_date) AS active_days, COUNT(DISTINCT space_id) AS spaces_used,
       COUNT(DISTINCT conversation_id) AS conversations,
       datediff(current_date(), MAX(event_date)) AS days_since_last,
       CASE WHEN COUNT(DISTINCT event_date) >= 8 THEN 'Power (8+ days)' WHEN COUNT(DISTINCT event_date) >= 3 THEN 'Regular (3-7 days)' ELSE 'Casual (1-2 days)' END AS engagement_tier
FROM q GROUP BY user_email ORDER BY questions DESC
""", None),
    "ds_new_returning": ("New vs returning users (weekly)", f"""
WITH a AS (SELECT user_email, event_date FROM g_audit_events WHERE event_category IN {QUESTION_CATS}),
f AS (SELECT user_email, MIN(event_date) AS first_date FROM a GROUP BY 1),   -- first-ever question (full lookback)
q AS (SELECT * FROM a WHERE event_date BETWEEN :date_range.min AND :date_range.max)
SELECT date_trunc('WEEK', q.event_date) AS week,
       CASE WHEN date_trunc('WEEK', f.first_date) = date_trunc('WEEK', q.event_date) THEN 'New' ELSE 'Returning' END AS user_type,
       COUNT(DISTINCT q.user_email) AS users
FROM q JOIN f USING (user_email) GROUP BY 1, 2 ORDER BY 1
""", None),
    "ds_freshness": ("Harvest freshness", """
SELECT MAX(run_ts) AS last_harvest, MAX_BY(n_spaces, run_ts) AS spaces, MAX_BY(n_conversations, run_ts) AS conversations,
       (SELECT COUNT(*) FROM genie_messages) AS messages_harvested, (SELECT COUNT(DISTINCT workspace_id) FROM genie_harvest_log) AS workspaces
FROM genie_harvest_log
""", None),
}

# ----------------------------------------------------------------------------------------------- widget helpers
_n = [0]


def _name(prefix):
    _n[0] += 1
    return f"{prefix}_{_n[0]}"


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(lines, x, y, w, h):
    return {"widget": {"name": _name("txt"), "multilineTextboxSpec": {"lines": lines}}, "position": pos(x, y, w, h)}


def F(name, expr):
    return {"name": name, "expression": expr}


def query(ds, fields, disagg=False, filters=None):
    q = {"datasetName": ds, "fields": fields, "disaggregated": disagg}
    if filters:
        q["filters"] = [{"expression": f} for f in filters]
    return [{"name": "main_query", "query": q}]


def frame(title, desc=None):
    f = {"showTitle": True, "title": title}
    if desc:
        f["description"], f["showDescription"] = desc, True
    return f


PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
USD = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 2}}
CNT = {"type": "number", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
SEC = {"type": "number", "decimalPlaces": {"type": "max", "places": 1}}


def counter(ds, title, value_field, fmt=None, period=None, desc=None, filters=None, x=0, y=0, w=2, h=3, bg=None):
    fields = [value_field] + ([period] if period else [])
    enc = {"value": {"fieldName": value_field["name"], "displayName": title}}
    if fmt:
        enc["value"]["format"] = fmt
    if period:
        enc["period"] = {"fieldName": period["name"]}
    spec = {"version": 2, "widgetType": "counter", "encodings": enc, "frame": frame(title, desc)}
    widget = {"name": _name("kpi"), "queries": query(ds, fields, filters=filters), "spec": spec}
    if bg:  # solid coloured KPI tile with white text
        spec["style"] = {"fontColor": {"light": "#FFFFFF", "dark": "#FFFFFF"}}
        widget["specExtensions"] = {"widgetBackgroundColor": {"light": bg, "dark": bg}}
    return {"widget": widget, "position": pos(x, y, w, h)}


DATE_FMT = {"type": "date", "date": "locale-short-month"}
TILE = {"blue": "#1D70B1", "teal": "#0B7A75", "navy": "#1B3139", "green": "#2E8540", "orange": "#C25E00", "purple": "#6A3D9A", "red": "#B3261E"}


def header(title, lines, h=4, minis=()):
    """9-wide intro text + From/To date tiles (and up to 2 mini KPIs) stacked on the right, like the reference dashboard."""
    out = [text([f"### {title}\n", "\n"] + lines, 0, 0, 9, h),
           counter("ds_date_range", "From", F("min(from_date)", "MIN(`from_date`)"), DATE_FMT, x=9, y=0, w=3, h=1),
           counter("ds_date_range", "To", F("max(to_date)", "MAX(`to_date`)"), DATE_FMT, x=9, y=1, w=3, h=1)]
    for i, (ds, t, fld, fmt) in enumerate(minis[:2]):
        out.append(counter(ds, t, fld, fmt, x=9, y=2 + i, w=3, h=1))
    return out


def chart(kind, ds, title, x_f, y_fs, color_f=None, x_type="temporal", y_fmt=None, desc=None, filters=None,
          horizontal=False, layout=None, pos_=(0, 0, 6, 5), color_map=None, sort_value=False):
    fields = [x_f] + y_fs + ([color_f] if color_f else [])
    xs = {"fieldName": x_f["name"], "scale": {"type": x_type}, "displayName": x_f.get("label", x_f["name"])}
    if sort_value and x_type == "categorical":
        xs["scale"]["sort"] = {"by": "y-reversed"}
    ys = {"scale": {"type": "quantitative"}}
    if len(y_fs) == 1:
        ys.update({"fieldName": y_fs[0]["name"], "displayName": y_fs[0].get("label", y_fs[0]["name"])})
        if y_fmt:
            ys["format"] = y_fmt
    else:
        ys["fields"] = [{"fieldName": f["name"], "displayName": f.get("label", f["name"])} for f in y_fs]
    enc = {"x": ys, "y": xs} if horizontal else {"x": xs, "y": ys}
    if color_f:
        enc["color"] = {"fieldName": color_f["name"], "scale": {"type": "categorical"}, "displayName": color_f.get("label", color_f["name"])}
        if color_map:
            enc["color"]["scale"]["mappings"] = [{"value": k, "color": v} for k, v in color_map.items()]
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": frame(title, desc)}
    if layout:
        spec["mark"] = {"layout": layout}
    clean = [{k: v for k, v in f.items() if k != "label"} for f in fields]
    return {"widget": {"name": _name(kind), "queries": query(ds, clean, filters=filters), "spec": spec}, "position": pos(*pos_)}


def pie(ds, title, angle_f, color_f, pos_, filters=None, color_map=None):
    enc = {"angle": {"fieldName": angle_f["name"], "scale": {"type": "quantitative"}, "displayName": angle_f.get("label", angle_f["name"])},
           "color": {"fieldName": color_f["name"], "scale": {"type": "categorical"}, "displayName": color_f.get("label", color_f["name"])},
           "label": {"show": True}}
    if color_map:
        enc["color"]["scale"]["mappings"] = [{"value": k, "color": v} for k, v in color_map.items()]
    clean = [{k: v for k, v in f.items() if k != "label"} for f in (angle_f, color_f)]
    return {"widget": {"name": _name("pie"), "queries": query(ds, clean, filters=filters),
                       "spec": {"version": 3, "widgetType": "pie", "encodings": enc, "frame": frame(title)}}, "position": pos(*pos_)}


def heatmap(ds, title, x_f, y_f, v_f, pos_, filters=None, desc=None):
    enc = {"x": {"fieldName": x_f["name"], "scale": {"type": "categorical"}, "displayName": x_f.get("label", x_f["name"])},
           "y": {"fieldName": y_f["name"], "scale": {"type": "categorical",
                                                     "sort": {"by": "custom-order", "orderedValues": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}},
                 "displayName": y_f.get("label", y_f["name"])},
           "color": {"fieldName": v_f["name"], "scale": {"type": "quantitative",
                                                         "colorRamp": {"mode": "custom-sequential", "colors": {"start": "#E8F1FA", "end": "#1B3139"}}},
                     "displayName": v_f.get("label", v_f["name"])}}
    clean = [{k: v for k, v in f.items() if k != "label"} for f in (x_f, y_f, v_f)]
    return {"widget": {"name": _name("heat"), "queries": query(ds, clean, filters=filters),
                       "spec": {"version": 3, "widgetType": "heatmap", "encodings": enc, "frame": frame(title, desc)}}, "position": pos(*pos_)}


def table(ds, title, cols, pos_, desc=None, filters=None):
    """cols: list of (field, display, fmt|None)"""
    fields = [F(c, f"`{c}`") for c, _, _ in cols]
    enc_cols = []
    for c, d, fmt in cols:
        e = {"fieldName": c, "displayName": d}
        if fmt:
            e["format"] = fmt
        enc_cols.append(e)
    return {"widget": {"name": _name("tbl"), "queries": query(ds, fields, disagg=True, filters=filters),
                       "spec": {"version": 2, "widgetType": "table", "encodings": {"columns": enc_cols}, "frame": frame(title, desc)}},
            "position": pos(*pos_)}


def filt(kind, title, bindings, pos_):
    """bindings: list of (dataset, field)"""
    queries, fields = [], []
    for i, (ds, fld) in enumerate(bindings):
        qn = f"f_{ds}_{fld}"
        queries.append({"name": qn, "query": {"datasetName": ds, "fields": [F(fld, f"`{fld}`")], "disaggregated": False}})
        fields.append({"fieldName": fld, "displayName": title, "queryName": qn})
    return {"widget": {"name": _name("flt"), "queries": queries,
                       "spec": {"version": 2, "widgetType": kind, "encodings": {"fields": fields}, "frame": frame(title)}},
            "position": pos(*pos_)}


def page(name, display, layout):
    return {"name": name, "displayName": display, "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": layout}


# common fields
day_a = {**F("daily(event_date)", 'DATE_TRUNC("DAY", `event_date`)'), "label": "Day"}
week_a = {**F("weekly(event_date)", 'DATE_TRUNC("WEEK", `event_date`)'), "label": "Week"}
week_m = {**F("weekly(created_date)", 'DATE_TRUNC("WEEK", `created_date`)'), "label": "Week"}
users_a = {**F("countdistinct(asking_user)", "COUNT(DISTINCT `asking_user`)"), "label": "Active users"}
questions_a = {**F("sum(is_question)", "SUM(`is_question`)"), "label": "Questions"}
Q_FILTER = [f"`event_category` IN {QUESTION_CATS}"]
OUTCOME_COLORS = {"Answered with SQL": "#00A972", "Text-only answer": "#4A8CC7", "Asked clarification": "#FFAB00",
                  "SQL error": "#FF7E5C", "Failed": "#C42B1C", "No answer": "#98102A", "Cancelled": "#8F8F8F", "In progress / other": "#C4C4C4"}

# style borrowed from the FE "Genie Usage Dashboard" (Inter, rounded cards, coloured KPI tiles, From/To header)
THEME = {"canvasBackgroundColor": {"light": "#E6E9EA", "dark": "#1E2931"}, "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#15181C"}, "widgetBorderColor": {"light": "#F3F5F6", "dark": "#21292F"}, "fontColor": {"light": "#1A2127", "dark": "#EEF0F2"}, "selectionColor": {"light": "#0E7AB9", "dark": "#41ADEC"}, "visualizationColors": ["#A6CEE3", "#1F78B4", "#B2DF8A", "#33A02C", "#FB9A99", "#E31A1C", "#FDBF6F", "#FF7F00", "#CAB2D6", "#6A3D9A", "#FFFF99", "#B15928"], "widgetHeaderAlignment": "LEFT", "fontFamily": "Inter", "widgetPadding": 16, "widgetMargin": 8, "widgetCornerRadius": 16, "widgetShadow": 50, "continuousGradient": {"lightMode": {"fromCategoricalIndex": 0}, "darkMode": {"fromCategoricalIndex": 0}}, "fontSettings": {"base": {"fontFamily": "Inter", "fontColor": {"light": "#1A2127", "dark": "#EEF0F2"}, "fontSize": 14, "fontWeight": 400}, "widgetTitle": {"fontSize": 16, "fontWeight": 500}, "widgetDescription": {"fontSize": 14, "fontWeight": 400}, "fieldTitle": {"textCase": "TEXT_CASE_UPPERCASE", "fontColor": {"light": "#31373D", "dark": "#D6D8DA"}, "fontSize": 13, "fontWeight": 400}, "fieldValue": {"fontColor": {"light": "#31373D", "dark": "#D6D8DA"}, "fontSize": 13, "fontWeight": 400}}, "gridLineColor": {"light": "#D6D7D8", "dark": "#3C3E42"}, "axisLineColor": {"light": "#B0B4B6", "dark": "#555E64"}}


# ----------------------------------------------------------------------------------------------- pages
C = TILE
M = lambda n: F(f"measure({n})", f"MEASURE(`{n}`)")
cost_sql = F("sum(warehouse_cost_usd)", "SUM(`warehouse_cost_usd`)")
cost_llm = F("sum(list_cost_usd)", "SUM(`list_cost_usd`)")
msgs = {**F("count(message_id)", "COUNT(`message_id`)"), "label": "Messages"}
space_x = {**F("space_title", "`space_title`"), "label": "Space"}
outcome_c = {**F("outcome", "`outcome`"), "label": "Outcome"}

overview = header("Genie Observability", [
    "Adoption, answer quality, reliability and cost for every **Genie space (Genie Agent)** in the workspace. ",
    "Activity comes from the **audit log** (all users, UI + API); answer quality from the **Genie API harvest**; cost from **billing + query history**.\n", "\n",
    "Use the **Date range** and **Genie space** global filters. Metric definitions are on the *About* page. ",
    "*System tables can lag by a few hours.*"],
    minis=[("ds_freshness", "Last harvest", F("max(last_harvest)", "MAX(`last_harvest`)"), DATE_FMT),
           ("ds_activity", "Spaces used", F("countdistinct(asked_space)", "COUNT(DISTINCT `asked_space`)"), None)]) + [
    counter("ds_activity", "Active users", users_a, CNT, desc="Distinct users who asked a question", x=0, y=4, w=4, bg=C["blue"]),
    counter("ds_activity", "Questions asked", questions_a, CNT, desc="UI + API, chat + agent mode", x=4, y=4, w=4, bg=C["blue"]),
    counter("ds_sql_cost", "Genie SQL cost ($)", cost_sql, USD, desc="Warehouse cost attributed to Genie SQL", x=8, y=4, w=4, bg=C["blue"]),
    counter("ds_messages", "Completion rate", M("Completion rate"), PCT, desc="Messages Genie completed", x=0, y=7, w=4, bg=C["teal"]),
    counter("ds_messages", "Positive feedback", M("Positive feedback rate"), PCT, desc="👍 ÷ (👍 + 👎)", x=4, y=7, w=4, bg=C["teal"]),
    counter("ds_messages", "Median response time (s)", M("Median response (s)"), SEC, desc="Question → answer, end to end", x=8, y=7, w=4, bg=C["teal"]),
    chart("bar", "ds_activity", "Questions per day by space", day_a, [questions_a], color_f={**F("asked_space", "`asked_space`"), "label": "Space"},
          filters=Q_FILTER, desc="All question events from the audit log", pos_=(0, 10, 12, 7)),
    chart("bar", "ds_activity", "Daily active users", day_a, [users_a], pos_=(0, 17, 6, 6)),
    chart("bar", "ds_messages", "Answer outcomes per week", week_m, [msgs], color_f=outcome_c, color_map=OUTCOME_COLORS, pos_=(6, 17, 6, 6)),
    table("ds_scorecard", "Space scorecard (last 30 days)", [
        ("space_title", "Space", None), ("active_users_30d", "Active users", None), ("questions_30d", "Questions", None),
        ("completion_rate_30d", "Completion", PCT), ("positive_feedback_rate_30d", "👍 rate", PCT), ("feedback_coverage_30d", "Rated", PCT),
        ("p50_response_sec_30d", "p50 latency (s)", SEC), ("latest_benchmark_accuracy", "Benchmark acc.", PCT),
        ("config_score", "Config score", None), ("total_cost_usd_30d", "Cost 30d", USD), ("health_flags", "Health flags", None)],
        (0, 23, 12, 8), desc="Fixed 30-day window; space filter applies"),
]

adoption = header("Adoption & Engagement", [
    "Who is using Genie, how often and when. Source: `system.access.audit` — every UI and API question, with the user's email, ",
    "independent of whether the harvest job can read the conversation.\n", "\n",
    "*Genie One* = the workspace-level Genie chat (not tied to a space)."],
    minis=[("ds_activity", "Active users", users_a, None), ("ds_activity", "Questions", questions_a, None)]) + [
    chart("bar", "ds_new_returning", "New vs returning users per week", {**F("week", "`week`"), "label": "Week"},
          [{**F("sum(users)", "SUM(`users`)"), "label": "Users"}], color_f={**F("user_type", "`user_type`"), "label": "User type"},
          color_map={"New": "#FF7F00", "Returning": "#1F78B4"}, pos_=(0, 4, 12, 7),
          desc="New = first-ever question that week. All spaces (date filter applies; space filter does not)"),
    heatmap("ds_activity", "When do people ask? Questions by hour × weekday (UTC)", {**F("hour_of_day", "`hour_of_day`"), "label": "Hour (UTC)"},
            {**F("day_of_week", "`day_of_week`"), "label": "Day"}, {**F("sum(is_question)", "SUM(`is_question`)"), "label": "Questions"},
            pos_=(0, 11, 8, 6), filters=Q_FILTER),
    pie("ds_users", "Engagement tiers (all spaces)", {**F("count(user_email)", "COUNT(`user_email`)"), "label": "Users"},
        {**F("engagement_tier", "`engagement_tier`"), "label": "Tier"}, (8, 11, 4, 6)),
    chart("bar", "ds_activity", "Questions by channel", day_a, [questions_a], color_f={**F("channel", "`channel`"), "label": "Channel"},
          filters=Q_FILTER, pos_=(0, 17, 6, 6), desc="UI = Genie UI / embedded; API = Conversation API (apps, Slack/Teams bots, agents)"),
    chart("bar", "ds_activity", "Chat vs agent mode", day_a, [questions_a], color_f={**F("event_category", "`event_category`"), "label": "Mode"},
          filters=Q_FILTER, pos_=(6, 17, 6, 6), desc="AGENT_MODE_QUESTION = multi-step agent / research mode"),
    chart("bar", "ds_activity", "Genie One — questions per day", day_a,
          [{**F("sum(is_genie_one_question)", "SUM(`is_genie_one_question`)"), "label": "Genie One questions"}], pos_=(0, 23, 12, 6)),
    table("ds_users", "User-level breakdown (all spaces)", [("user_email", "User", None), ("questions", "Questions", None), ("conversations", "Conversations", None),
                                               ("active_days", "Active days", None), ("spaces_used", "Spaces", None), ("first_seen", "First seen", None),
                                               ("last_seen", "Last seen", None), ("days_since_last", "Days since last", None), ("engagement_tier", "Tier", None)],
          (0, 29, 12, 10)),
]

quality = header("Answer Quality & Reliability", [
    "From the Genie API harvest: message status, attachments, clarifications, errors and feedback, plus the SQL Genie ran ",
    "(joined to `system.query.history` by statement ID) to split response time into **Genie reasoning** vs **SQL execution**.\n", "\n",
    "Use the *Problem answers* table to find questions to turn into instructions, example SQL or benchmarks."],
    minis=[("ds_messages", "Messages analysed", msgs, None), ("ds_messages", "Review requests", F("sum(n_review_requests)", "SUM(`n_review_requests`)"), None)]) + [
    counter("ds_messages", "Completion rate", M("Completion rate"), PCT, x=0, y=4, w=4, bg=C["teal"]),
    counter("ds_messages", "Positive feedback", M("Positive feedback rate"), PCT, x=4, y=4, w=4, bg=C["teal"]),
    counter("ds_messages", "P90 response time (s)", M("P90 response (s)"), SEC, x=8, y=4, w=4, bg=C["orange"]),
    chart("bar", "ds_messages", "Answer outcomes per day", {**F("daily(created_date)", 'DATE_TRUNC("DAY", `created_date`)'), "label": "Day"}, [msgs],
          color_f=outcome_c, color_map=OUTCOME_COLORS, pos_=(0, 7, 12, 7)),
    chart("bar", "ds_messages", "Outcome mix by space", space_x, [msgs], color_f=outcome_c, color_map=OUTCOME_COLORS, x_type="categorical",
          horizontal=True, layout="stack-100", pos_=(0, 14, 6, 7)),
    chart("bar", "ds_messages", "Response-time distribution", {**F("latency_bucket", "`latency_bucket`"), "label": "Response time"}, [msgs],
          x_type="categorical", pos_=(6, 14, 6, 7)),
    chart("bar", "ds_messages", "Where does the time go? Avg seconds per answer", space_x,
          [{**F("avg(genie_reasoning_sec)", "AVG(`genie_reasoning_sec`)"), "label": "Genie reasoning"},
           {**F("avg(sql_queue_sec)", "AVG(`sql_queue_sec`)"), "label": "SQL queue"},
           {**F("avg(sql_total_sec)", "AVG(`sql_total_sec`)"), "label": "SQL total"}],
          x_type="categorical", horizontal=True, layout="group", pos_=(0, 21, 6, 7),
          desc="High SQL queue → warehouse undersized or cold; high reasoning → complex space / many tables"),
    chart("bar", "ds_messages", "Feedback per week", week_m,
          [{**F("sum(thumbs_up)", "SUM(`thumbs_up`)"), "label": "👍"}, {**F("sum(thumbs_down)", "SUM(`thumbs_down`)"), "label": "👎"}],
          layout="group", pos_=(6, 21, 6, 7)),
    table("ds_messages", "Problem answers — failed, SQL error, no answer, or 👎", [
        ("created_ts", "When", None), ("space_title", "Space", None), ("user_email", "User", None), ("question", "Question", None),
        ("outcome", "Outcome", None), ("feedback_rating", "Feedback", None), ("error_type", "Error type", None),
        ("error_message", "Error", None), ("sql", "Generated SQL", None)],
        (0, 28, 12, 10), filters=["`is_bad_outcome` = 1 OR `thumbs_down` = 1"]),
]

health = header("Space Health & Configuration", [
    "Configuration maturity from each space's serialized definition (instructions, example SQL, joins, sample questions, benchmarks), ",
    "benchmark accuracy from the Genie eval API, reach (ACLs) vs adoption, and tables that are attached but never used.\n", "\n",
    "**Health flags** call out what to fix first."],
    minis=[("ds_scorecard", "Spaces", F("count(space_id)", "COUNT(`space_id`)"), None),
           ("ds_scorecard", "Avg config score", F("avg(config_score)", "AVG(`config_score`)"), {"type": "number", "decimalPlaces": {"type": "max", "places": 0}})]) + [
    table("ds_scorecard", "Configuration, reach & health", [
        ("space_title", "Space", None), ("health_flags", "Health flags", None), ("config_score", "Config score", None),
        ("n_tables", "Tables", None), ("n_text_instructions", "Instructions", None), ("instruction_chars", "Instr. chars", None),
        ("n_example_sqls", "Example SQL", None), ("n_join_specs", "Joins", None), ("n_sample_questions", "Sample Qs", None),
        ("n_benchmark_questions", "Benchmarks", None), ("latest_benchmark_accuracy", "Benchmark acc.", PCT),
        ("n_users_with_access", "Users w/ access", None), ("n_groups_with_access", "Groups w/ access", None),
        ("active_users_30d", "Active 30d", None), ("adoption_rate_30d", "Adoption (direct grants only)", PCT), ("days_since_last_question", "Days idle", None), ("config_changes_30d", "Edits 30d", None),
        ("creator_email", "Creator", None), ("harvest_scope", "Harvest scope", None), ("is_active", "Active", None)], (0, 4, 12, 9)),
    chart("bar", "ds_scorecard", "Configuration score by space", space_x, [{**F("max(config_score)", "MAX(`config_score`)"), "label": "Config score"}],
          x_type="categorical", horizontal=True, pos_=(0, 13, 6, 7), sort_value=True),
    chart("line", "ds_evals", "Benchmark accuracy over time", {**F("run_date", "`run_date`"), "label": "Run date"},
          [{**F("avg(accuracy)", "AVG(`accuracy`)"), "label": "Accuracy"}], color_f=space_x, y_fmt=PCT, pos_=(6, 13, 6, 7)),
    table("ds_tables", "Attached tables vs tables Genie actually used (90 days)", [
        ("space_title", "Space", None), ("table_identifier", "Table", None), ("n_messages_using_90d", "Messages using", None), ("harvest_scope", "Harvest scope", None),
        ("last_used_ts", "Last used", None), ("usage_status", "Status", None), ("n_column_configs", "Column configs", None)], (0, 20, 12, 8)),
]

cost = header("Cost", [
    "**Genie SQL cost** — each Genie statement's share (by task time) of its SQL warehouse's hourly cost.\n", "\n",
    "**Genie LLM cost** — `system.billing.usage` where `billing_origin_product = 'GENIE'`, split by surface (Agents / One / Code) and attributed to a space via `usage_metadata.genie.agent_id`. ",
    "List prices, USD; `GENIE_FREE_USAGE` is the free allowance."],
    minis=[("ds_sql_cost", "SQL statements", F("sum(statements)", "SUM(`statements`)"), CNT), ("ds_billing", "LLM DBUs", F("sum(dbus)", "SUM(`dbus`)"), CNT)]) + [
    counter("ds_sql_cost", "Genie SQL cost ($)", cost_sql, USD, x=0, y=4, w=4, bg=C["blue"], desc="Attributed SQL warehouse cost"),
    counter("ds_billing", "Genie LLM cost ($)", cost_llm, USD, x=4, y=4, w=4, bg=C["blue"], desc="All Genie surfaces, list price (free allowance = $0)"),
    counter("ds_billing", "Free allowance used (DBUs)", F("sum(dbus)", "SUM(`dbus`)"), CNT, x=8, y=4, w=4, bg=C["purple"],
            desc="GENIE_FREE_USAGE DBUs (not charged)", filters=["`billing_type` = 'Free allowance'"]),
    chart("bar", "ds_sql_cost", "Genie SQL cost per day by space", {**F("query_date", "`query_date`"), "label": "Day"},
          [{**cost_sql, "label": "Cost (USD)"}], color_f=space_x, y_fmt=USD, pos_=(0, 7, 12, 7)),
    chart("bar", "ds_billing", "Genie LLM usage per day by surface (DBUs)", {**F("usage_date", "`usage_date`"), "label": "Day"},
          [{**F("sum(dbus)", "SUM(`dbus`)"), "label": "DBUs"}], color_f={**F("surface", "`surface`"), "label": "Surface"}, pos_=(0, 14, 12, 7),
          desc="Includes paid and free DBUs"),
    chart("bar", "ds_sql_cost", "SQL cost by space", space_x, [{**cost_sql, "label": "Cost (USD)"}], x_type="categorical", horizontal=True,
          y_fmt=USD, pos_=(0, 21, 6, 8), sort_value=True),
    chart("bar", "ds_billing", "LLM cost by space (Genie Agents)", space_x, [{**cost_llm, "label": "List cost (USD)"}], x_type="categorical",
          horizontal=True, y_fmt=USD, pos_=(6, 21, 6, 8), sort_value=True, filters=["`surface` = 'GENIE_AGENTS'"]),
    table("ds_billing", "LLM usage by user / service principal", [("usage_date", "Date", None), ("surface", "Surface", None), ("channel", "Channel", None),
                                                                  ("space_title", "Space", None), ("run_as", "User / SP", None),
                                                                  ("billing_type", "Type", None), ("dbus", "DBUs", CNT), ("list_cost_usd", "List cost", USD)],
          (0, 29, 12, 9)),
    table("ds_sql_cost", "SQL cost by identity (who the SQL ran as)", [("query_date", "Date", None), ("space_title", "Space", None),
                                                                     ("executed_by", "Executed by", None), ("statements", "Statements", None),
                                                                     ("failed_statements", "Failed", None), ("cached_statements", "Cache hits", None),
                                                                     ("warehouse_cost_usd", "Cost", USD)],
          (0, 38, 12, 9), desc="A service principal here means the space uses embedded credentials"),
]

explorer = header("Question Explorer", [
    "Every harvested question with Genie's answer, SQL, outcome and feedback.\n", "\n",
    "⚠️ Contains user-typed text and business data — restrict access to this dashboard and/or apply column masks to `genie_messages` / `g_messages`."],
    minis=[("ds_messages", "Questions", msgs, None), ("ds_messages", "Users", F("countdistinct(user_email)", "COUNT(DISTINCT `user_email`)"), None)]) + [
    filt("filter-text-entry", "Question contains", [("ds_messages", "question")], (0, 4, 4, 2)),
    filt("filter-multi-select", "User", [("ds_messages", "user_email")], (4, 4, 4, 2)),
    filt("filter-multi-select", "Outcome", [("ds_messages", "outcome")], (8, 4, 4, 2)),
    table("ds_messages", "Questions", [
        ("created_ts", "When", None), ("space_title", "Space", None), ("user_email", "User", None), ("turn_type", "Turn", None),
        ("agent_type", "Mode", None), ("question", "Question", None), ("outcome", "Outcome", None), ("response_time_sec", "Resp. (s)", SEC),
        ("row_count", "Rows", None), ("feedback_rating", "Feedback", None), ("sql_title", "SQL title", None), ("sql", "SQL", None),
        ("text_response", "Answer text", None), ("sql_cost_usd", "SQL cost", USD), ("conversation_id", "Conversation", None)], (0, 6, 12, 16)),
]

POS_NEG = {"👍 Positive": "#2E8540", "👎 Negative": "#E31A1C"}
rating_c = {**F("rating_label", "`rating_label`"), "label": "Rating"}
n_ratings = {**F("sum(ratings)", "SUM(`ratings`)"), "label": "Ratings"}
fb_week = {**F("weekly(feedback_date)", 'DATE_TRUNC("WEEK", `feedback_date`)'), "label": "Week"}
feedback = header("Feedback (👍 / 👎)", [
    "Every thumbs-up / thumbs-down users gave on Genie answers, from the audit log (Genie UI, Conversation API and Genie One) ",
    "normalised to one **latest rating per rater per answer**, then joined to the rated question, its outcome, latency and SQL.\n", "\n",
    "Use it to find spaces and question types that disappoint, check whether slow or clarifying answers drive 👎, ",
    "and turn 👎 questions into instructions, example SQL or benchmarks."],
    minis=[("ds_feedback", "Ratings", n_ratings, None),
           ("ds_feedback", "Raters", F("countdistinct(rater_email)", "COUNT(DISTINCT `rater_email`)"), None)]) + [
    counter("ds_feedback", "👍 Thumbs up", F("sum(thumbs_up)", "SUM(`thumbs_up`)"), CNT, x=0, y=4, w=3, bg=C["green"]),
    counter("ds_feedback", "👎 Thumbs down", F("sum(thumbs_down)", "SUM(`thumbs_down`)"), CNT, x=3, y=4, w=3, bg=C["red"]),
    counter("ds_feedback", "Positive rate", M("Positive rate"), PCT, x=6, y=4, w=3, bg=C["teal"], desc="👍 ÷ (👍 + 👎)"),
    counter("ds_messages", "Feedback coverage", M("Feedback coverage"), PCT, x=9, y=4, w=3, bg=C["navy"],
            desc="Share of Genie-space answers that were rated"),
    chart("bar", "ds_feedback", "Ratings per week", fb_week, [n_ratings], color_f=rating_c, color_map=POS_NEG, pos_=(0, 7, 8, 7)),
    chart("line", "ds_feedback", "Positive rate per week", fb_week, [{**M("Positive rate"), "label": "Positive rate"}], y_fmt=PCT, pos_=(8, 7, 4, 7)),
    chart("bar", "ds_feedback", "👍 vs 👎 by space", space_x, [n_ratings], color_f=rating_c, color_map=POS_NEG, x_type="categorical",
          horizontal=True, pos_=(0, 14, 6, 7), sort_value=True),
    chart("bar", "ds_feedback", "Positive rate by space", space_x, [{**M("Positive rate"), "label": "Positive rate"}], x_type="categorical",
          horizontal=True, y_fmt=PCT, pos_=(6, 14, 6, 7), sort_value=True),
    chart("bar", "ds_feedback", "Rating by answer outcome", {**F("outcome", "`outcome`"), "label": "Outcome"}, [n_ratings], color_f=rating_c,
          color_map=POS_NEG, x_type="categorical", layout="stack-100", pos_=(0, 21, 4, 7),
          desc="Do clarifications / text-only answers get more 👎?"),
    chart("bar", "ds_feedback", "Rating by response time", {**F("latency_bucket", "`latency_bucket`"), "label": "Response time"}, [n_ratings],
          color_f=rating_c, color_map=POS_NEG, x_type="categorical", layout="stack-100", pos_=(4, 21, 4, 7),
          desc="Does slowness drive 👎?"),
    chart("bar", "ds_feedback", "Rating by conversation turn", {**F("turn_type", "`turn_type`"), "label": "Turn"}, [n_ratings],
          color_f=rating_c, color_map=POS_NEG, x_type="categorical", layout="stack-100", pos_=(8, 21, 4, 7),
          desc="First question vs follow-ups"),
    chart("bar", "ds_feedback", "Ratings by surface & channel", {**F("surface", "`surface`"), "label": "Surface"}, [n_ratings],
          color_f={**F("channel", "`channel`"), "label": "Channel"}, x_type="categorical", pos_=(0, 28, 4, 6),
          desc="Genie space vs Genie One; UI vs API"),
    pie("ds_feedback", "Who rates? Own vs others' answers", n_ratings, {**F("rater_relation", "`rater_relation`"), "label": "Rated"}, (4, 28, 4, 6)),
    chart("bar", "ds_feedback", "Time to feedback (median minutes) by rating", {**F("rating_label", "`rating_label`"), "label": "Rating"},
          [{**M("Median minutes to feedback"), "label": "Median minutes"}], x_type="categorical", pos_=(8, 28, 4, 6),
          desc="From answer to first rating"),
    table("ds_raters", "Feedback by user (all spaces)", [("rater_email", "User", None), ("surface", "Surface", None), ("ratings", "Ratings", None),
                                            ("thumbs_up", "👍", None), ("thumbs_down", "👎", None), ("positive_rate", "Positive rate", PCT),
                                            ("rating_changes", "Changed rating", None), ("spaces_rated", "Spaces", None),
                                            ("first_feedback", "First", None), ("last_feedback", "Last", None)], (0, 34, 12, 8)),
    table("ds_feedback", "👎 Thumbs-down answers — what went wrong?", [
        ("feedback_ts", "Rated at", None), ("space_title", "Space", None), ("rater_email", "Rated by", None), ("asker_email", "Asked by", None),
        ("question", "Question", None), ("outcome", "Outcome", None), ("response_time_sec", "Resp. (s)", SEC), ("row_count", "Rows", None),
        ("review_requested", "Review requested", None), ("sql", "Generated SQL", None), ("text_response", "Answer", None)],
        (0, 42, 12, 10), filters=["`thumbs_down` = 1"]),
    table("ds_feedback", "👍 Thumbs-up answers — candidates for example SQL / benchmarks", [
        ("feedback_ts", "Rated at", None), ("space_title", "Space", None), ("rater_email", "Rated by", None),
        ("question", "Question", None), ("sql", "Generated SQL", None)], (0, 52, 12, 8), filters=["`thumbs_up` = 1"]),
]

LIFECYCLE = "('SPACE_CREATE','SPACE_CLONE','SPACE_TRASH','SPACE_CONFIG_CHANGE','BENCHMARK_RUN')"
SHARING = "('SHARE_CONVERSATION','EXPORT','DELETE_CONVERSATION','REVIEW_REQUEST','REGENERATE','CANCEL')"
evt = {**F("count(event_time)", "COUNT(`event_time`)"), "label": "Events"}
cat_c = {**F("event_category", "`event_category`"), "label": "Event"}
governance = header("Governance & Lifecycle", [
    "Space creation, cloning, trashing and configuration edits; benchmark runs; conversation sharing, exports and deletions; ",
    "and reach (who has access) vs adoption (who actually uses it). Source: audit log + permissions API."],
    minis=[("ds_activity", "Spaces created / cloned", F("count(event_time)", "COUNT(`event_time`)"), None)]) + [
    chart("bar", "ds_activity", "Space lifecycle & configuration events per day", day_a, [evt], color_f=cat_c,
          filters=[f"`event_category` IN {LIFECYCLE}"], pos_=(0, 4, 12, 7)),
    chart("bar", "ds_activity", "Sharing, exports, deletions & review requests per day", day_a, [evt], color_f=cat_c,
          filters=[f"`event_category` IN {SHARING}"], pos_=(0, 11, 12, 7)),
    chart("bar", "ds_scorecard", "Reach vs adoption — users with direct access vs active users (30d)", space_x,
          [{**F("sum(n_users_with_access)", "SUM(`n_users_with_access`)"), "label": "Users with direct access"},
           {**F("sum(active_users_30d)", "SUM(`active_users_30d`)"), "label": "Active users 30d"}],
          x_type="categorical", horizontal=True, layout="group", pos_=(0, 18, 12, 8),
          desc="Group grants are not expanded (see Groups w/ access on Space Health)"),
    table("ds_activity", "Lifecycle, configuration & sharing events", [("event_time", "When", None), ("event_category", "Event", None),
                                                                       ("space_title", "Space", None), ("user_email", "User", None),
                                                                       ("channel", "Channel", None), ("is_success", "Succeeded", None)], (0, 26, 12, 9),
          filters=[f"`event_category` IN ('SPACE_CREATE','SPACE_CLONE','SPACE_TRASH','SPACE_CONFIG_CHANGE','SHARE_CONVERSATION','EXPORT','DELETE_CONVERSATION')"]),
]
# header mini KPI (index 3) counts only create/clone events
governance[3]["widget"]["queries"][0]["query"]["filters"] = [{"expression": "`event_category` IN ('SPACE_CREATE','SPACE_CLONE')"}]

about = [
    text(["### About & Definitions\n", "\n",
          "**Pipeline** — job *Genie Observability — harvest & gold refresh* (daily, serverless). Task 1 calls the Genie REST API; task 2 rebuilds the `g_*` gold tables from the harvest plus `system.access.audit`, `system.query.history` and `system.billing.usage`.\n", "\n",
          "| Metric | Definition | Source |\n", "|---|---|---|\n",
          "| Active users | Distinct emails with a question event (`createConversationMessage`, `genieStartConversationMessage`, `genieCreateConversationMessage`, agent-mode responses) | audit |\n",
          "| Questions | Count of the question events above (UI + API) | audit |\n",
          "| Completion rate | Messages with status `COMPLETED` ÷ all harvested messages | API |\n",
          "| Outcome | Failed / Cancelled / Asked clarification / SQL error / Answered with SQL / Text-only | API + query history |\n",
          "| Positive feedback rate | 👍 ÷ (👍 + 👎) counting each rater's latest rating per answer (UI, API and Genie One ratings normalised); API rating used only when no audit rating exists | audit + API |\n",
          "| Response time | Message `last_updated − created`; *reasoning* = response time − SQL time | API + query history |\n",
          "| Config score (0-100) | 20 instructions + 20 example SQL (4 each, max 5) + 10 joins (or single table) + 10 sample questions + 20 benchmarks (2 each, max 10) + 10 for ≤25 tables + 10 column configs | serialized space |\n",
          "| Adoption rate | Active users 30d ÷ users with a direct ACL entry; blank when the space is shared via groups (not expanded) | permissions API + audit |\n",
          "| Genie SQL cost | Statement task time ÷ all task time on that warehouse-hour × warehouse-hour list cost | billing + query history |\n",
          "| Genie LLM cost | `billing_origin_product = 'GENIE'` list cost; space from `usage_metadata.genie.agent_id` | billing |\n",
          "| Benchmark accuracy | `num_correct ÷ num_questions` for a DONE eval run | eval API (Beta) |\n", "\n",
          "**Coverage** — the harvest sees every user's conversations only in spaces where the job principal has **CAN MANAGE** (see *Harvest scope*). ",
          "Audit-based metrics cover all users regardless. System tables are account-wide and are filtered to the harvested workspace(s)."], 0, 0, 12, 13),
    table("ds_freshness", "Data freshness", [("last_harvest", "Last harvest (UTC)", None), ("workspaces", "Workspaces", None), ("spaces", "Spaces", None),
                                             ("conversations", "Conversations", None), ("messages_harvested", "Messages harvested", None)], (0, 13, 12, 3)),
]

DATE_BINDINGS = [("ds_activity", "event_date"), ("ds_messages", "created_date"), ("ds_sql_cost", "query_date"), ("ds_billing", "usage_date"), ("ds_evals", "run_date"), ("ds_feedback", "feedback_date")]
SPACE_BINDINGS = [("ds_activity", "space_title"), ("ds_messages", "space_title"), ("ds_sql_cost", "space_title"), ("ds_billing", "space_title"),
                  ("ds_scorecard", "space_title"), ("ds_evals", "space_title"), ("ds_tables", "space_title"), ("ds_feedback", "space_title")]

date_filter = filt("filter-date-range-picker", "Date range", DATE_BINDINGS, (0, 0, 3, 2))
# also drive the From/To header tiles via the ds_date_range parameter
for ds in ("ds_date_range", "ds_users", "ds_new_returning", "ds_raters"):   # parameter-bound (pre-aggregated) datasets
    date_filter["widget"]["queries"].append({"name": f"p_{ds}", "query": {"datasetName": ds,
                                             "parameters": [{"name": "date_range", "keyword": "date_range"}], "disaggregated": False}})
    date_filter["widget"]["spec"]["encodings"]["fields"].append({"parameterName": "date_range", "queryName": f"p_{ds}"})
date_filter["widget"]["spec"]["selection"] = {"defaultSelection": {"range": {"dataType": "DATE", "min": {"value": "now-90d/d"}, "max": {"value": "now/d"}}}}
filters_page = {"name": "global_filters", "displayName": "Global Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layoutVersion": "GRID_V1",
                "layout": [date_filter, filt("filter-multi-select", "Genie space", SPACE_BINDINGS, (3, 0, 3, 2))]}

DATASETS["ds_date_range"] = ("Date range display", "SELECT CAST(:date_range.min AS DATE) AS from_date, CAST(:date_range.max AS DATE) AS to_date", None)
PARAMS = {k: [{"displayName": "Date Range", "keyword": "date_range", "dataType": "DATE", "complexType": "RANGE",
                "defaultSelection": {"range": {"dataType": "DATE", "min": {"value": "now-90d/d"}, "max": {"value": "now/d"}}}}]
          for k in ("ds_users", "ds_new_returning", "ds_raters")}
PARAMS |= {"ds_date_range": [{"displayName": "Date Range", "keyword": "date_range", "dataType": "DATE", "complexType": "RANGE",
                             "defaultSelection": {"range": {"dataType": "DATE", "min": {"value": "now-90d/d"}, "max": {"value": "now/d"}}}}]}

dashboard = {
    "datasets": [{"name": k, "displayName": v[0], "queryLines": [ln + "\n" for ln in v[1].strip().splitlines()], **({"columns": v[2]} if v[2] else {}), **({"parameters": PARAMS[k]} if k in PARAMS else {})}
                 for k, v in DATASETS.items()],
    "pages": [page("overview", "Overview", overview), page("adoption", "Adoption", adoption), page("quality", "Quality & Reliability", quality),
              page("health", "Space Health", health), page("feedback", "Feedback", feedback), page("cost", "Cost", cost), page("explorer", "Question Explorer", explorer),
              page("governance", "Governance", governance), page("about", "About", about), filters_page],
    "uiSettings": {"theme": THEME},
}

OUT.write_text(json.dumps(dashboard, indent=2))
print(f"wrote {OUT} — {len(dashboard['datasets'])} datasets, {sum(len(p['layout']) for p in dashboard['pages'])} widgets")
