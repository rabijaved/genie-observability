# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Observability — 02 Build gold tables (API harvest × system tables)
# MAGIC
# MAGIC Materialises the tables the dashboard reads. Each one is rebuilt in full on every run (`CREATE OR REPLACE`), so they are always consistent.
# MAGIC
# MAGIC | Gold table | Built from | What it answers |
# MAGIC |---|---|---|
# MAGIC | `g_audit_events` | `system.access.audit` (`aibiGenie`, `genieChat`) | every question / feedback / review request / share / space lifecycle & config change, with **email**. Covers *all* users even without `include_all` access |
# MAGIC | `g_query_stats` | `system.query.history` × `system.billing.usage` | every SQL statement Genie ran: latency breakdown, rows/bytes, failures, **warehouse cost attributed per statement** |
# MAGIC | `g_genie_billing` | `system.billing.usage` (`billing_origin_product = 'GENIE'`) | Genie LLM/serving DBUs by surface (Agents / One / Code), channel (UI/API), **space (agent_id)** and user |
# MAGIC | `g_messages` | `genie_messages` × users × `g_query_stats` × audit feedback | one row per question with outcome, latency split (LLM vs SQL), cost, feedback |
# MAGIC | `g_feedback` | audit feedback events (UI/API/Genie One, ratings normalised) × `g_messages` | latest 👍/👎 per rater per answer, with the rated question, outcome, latency, time-to-feedback |
# MAGIC | `g_space_scorecard` | everything | one row per space: config maturity, reach vs adoption, quality, benchmark accuracy, cost, health flags |
# MAGIC | `g_table_usage` | space config × message thoughts | attached tables vs tables Genie actually used |
# MAGIC | `g_eval_runs` | `genie_eval_runs` | benchmark accuracy trend |

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "genie_observability", "Schema")
dbutils.widgets.text("system_lookback_days", "180", "Days of system-table history")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
LB = int(dbutils.widgets.get("system_lookback_days") or 180)
FQ = f"`{CATALOG}`.`{SCHEMA}`"

# Workspaces covered = every workspace that has run the harvest into this schema (supports a central schema fed by many workspaces)
ws_ids = [r.workspace_id for r in spark.sql(f"SELECT DISTINCT workspace_id FROM {FQ}.genie_harvest_log").collect()]
WS = ",".join(f"'{w}'" for w in ws_ids) or "''"
print(f"Workspaces: {ws_ids}; lookback {LB} days")

def run(sql):
    spark.sql(sql.replace("{FQ}", FQ).replace("{WS}", WS).replace("{LB}", str(LB)))

# COMMAND ----------

# DBTITLE 1,g_audit_events — Genie activity from the audit log
run("""
CREATE OR REPLACE TABLE {FQ}.g_audit_events
COMMENT 'Genie (aibiGenie) and Genie One (genieChat) audit events, categorised. Source: system.access.audit'
AS
WITH a AS (
  SELECT workspace_id, event_date, event_time, service_name, action_name,
         user_identity.email AS user_email,
         -- createSpace/cloneSpace return the NEW space id in response.result (request space_id = clone source)
         CASE WHEN action_name IN ('createSpace','genieCreateSpace','cloneSpace')
              THEN COALESCE(get_json_object(response.result, '$.space_id'), request_params['space_id'])
              ELSE request_params['space_id'] END AS space_id,
         CASE WHEN action_name = 'cloneSpace' THEN request_params['space_id'] END AS cloned_from_space_id,
         request_params['conversation_id'] AS conversation_id,
         request_params['message_id'] AS message_id,
         COALESCE(request_params['feedback_rating'], request_params['rating'],
                  NULLIF(regexp_extract(request_params['feedback_payload'], 'feedback_rating"[ ]*:[ ]*"([A-Z_]+)"', 1), '')) AS feedback_rating_raw,
         -- UI sends THUMBS_UP/THUMBS_DOWN, API sends POSITIVE/NEGATIVE/NONE, Genie One sends ONE_CHAT_FEEDBACK_RATING_THUMBS_*
         CASE WHEN COALESCE(request_params['feedback_rating'], request_params['rating'],
                            NULLIF(regexp_extract(request_params['feedback_payload'], 'feedback_rating"[ ]*:[ ]*"([A-Z_]+)"', 1), '')) RLIKE 'POSITIVE|THUMBS_UP$' THEN 'POSITIVE'
              WHEN COALESCE(request_params['feedback_rating'], request_params['rating'],
                            NULLIF(regexp_extract(request_params['feedback_payload'], 'feedback_rating"[ ]*:[ ]*"([A-Z_]+)"', 1), '')) RLIKE 'NEGATIVE|THUMBS_DOWN$' THEN 'NEGATIVE'
              WHEN action_name IN ('updateConversationMessageFeedback','genieSendMessageFeedback','updateGenieChatConversationFeedback') THEN 'REMOVED'
         END AS feedback_rating,
         request_params['response_id'] AS response_id,
         request_params['source'] AS source,
         response.status_code AS status_code,
         response.error_message AS error_message,
         CASE
           WHEN action_name IN ('createConversationMessage','genieStartConversationMessage','genieCreateConversationMessage') THEN 'QUESTION'
           WHEN action_name IN ('createAgenticResponse','genieCreateAgenticResponse') THEN 'AGENT_MODE_QUESTION'
           WHEN action_name IN ('regenerateConversationMessage') THEN 'REGENERATE'
           WHEN action_name IN ('cancelMessage','genieCancelAgenticResponse') THEN 'CANCEL'
           WHEN action_name IN ('updateConversationMessageFeedback','genieSendMessageFeedback') THEN 'FEEDBACK'
           WHEN action_name IN ('createConversationMessageComment','genieCreateMessageComment') THEN 'REVIEW_REQUEST'
           WHEN action_name IN ('updateConversationShare') THEN 'SHARE_CONVERSATION'
           WHEN action_name IN ('deleteConversation','genieDeleteConversation') THEN 'DELETE_CONVERSATION'
           WHEN action_name IN ('genieDownloadVisualization','downloadQueryResult','genieGenerateDownloadFullQueryResult','executeFullQueryResult') THEN 'EXPORT'
           WHEN action_name IN ('createSpace','genieCreateSpace') THEN 'SPACE_CREATE'
           WHEN action_name IN ('cloneSpace') THEN 'SPACE_CLONE'
           WHEN action_name IN ('trashSpace','genieTrashSpace') THEN 'SPACE_TRASH'
           WHEN action_name IN ('updateSpace','genieUpdateSpace','addDataSources','removeDataSources','createInstruction','updateInstruction',
                                'deleteInstruction','createCuratedQuestion','updateCuratedQuestion','deleteCuratedQuestion',
                                'updateSampleQuestions','updateGenieColumnConfigs') THEN 'SPACE_CONFIG_CHANGE'
           WHEN action_name IN ('createEvaluationRun','genieCreateEvalRun') THEN 'BENCHMARK_RUN'
           WHEN action_name IN ('createGenieChatResponse') THEN 'GENIE_ONE_QUESTION'
           WHEN action_name IN ('updateGenieChatConversationFeedback') THEN 'GENIE_ONE_FEEDBACK'
           WHEN action_name IN ('mcpToolInvocation') THEN 'GENIE_ONE_TOOL_CALL'
           WHEN action_name IN ('createScheduledTask') THEN 'GENIE_ONE_SCHEDULED_TASK'
         END AS event_category,
         CASE WHEN action_name RLIKE '^genie[A-Z]' THEN 'API' ELSE 'UI' END AS channel
  FROM system.access.audit
  WHERE service_name IN ('aibiGenie','genieChat')
    AND event_date >= date_sub(current_date(), {LB})
    AND workspace_id IN ({WS})
)
SELECT a.*, s.title AS space_title,
       (a.status_code IS NULL OR a.status_code < 400) AS is_success
FROM a
LEFT JOIN {FQ}.genie_spaces s ON a.space_id = s.space_id AND a.workspace_id = s.workspace_id
WHERE a.event_category IS NOT NULL
""")

# COMMAND ----------

# DBTITLE 1,g_query_stats — every Genie SQL statement with attributed warehouse cost
run("""
CREATE OR REPLACE TABLE {FQ}.g_query_stats
COMMENT 'SQL statements executed by Genie spaces (system.query.history) with warehouse cost attributed by task-time share per warehouse-hour'
AS
WITH prices AS (
  SELECT sku_name, cloud, usage_unit, price_start_time, price_end_time,
         COALESCE(pricing.effective_list.default, pricing.default) AS usd_per_unit
  FROM system.billing.list_prices WHERE currency_code = 'USD'
),
wh_cost AS (       -- warehouse cost per hour
  SELECT u.workspace_id, u.usage_metadata.warehouse_id AS warehouse_id, date_trunc('HOUR', u.usage_start_time) AS hr,
         SUM(u.usage_quantity) AS dbus, SUM(u.usage_quantity * p.usd_per_unit) AS usd
  FROM system.billing.usage u
  JOIN prices p ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
   AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
  WHERE u.usage_date >= date_sub(current_date(), {LB}) AND u.workspace_id IN ({WS})
    AND u.usage_metadata.warehouse_id IN (SELECT DISTINCT warehouse_id FROM {FQ}.genie_spaces)
  GROUP BY 1,2,3
),
wh_work AS (       -- all work on those warehouses per hour (Genie and non-Genie)
  SELECT workspace_id, compute.warehouse_id AS warehouse_id, date_trunc('HOUR', start_time) AS hr,
         SUM(total_task_duration_ms) AS task_ms
  FROM system.query.history
  WHERE start_time >= date_sub(current_date(), {LB}) AND workspace_id IN ({WS})
    AND compute.warehouse_id IN (SELECT DISTINCT warehouse_id FROM {FQ}.genie_spaces)
  GROUP BY 1,2,3
),
q AS (
  SELECT workspace_id, statement_id, query_source.genie_space_id AS space_id, executed_by, executed_as,
         compute.warehouse_id AS warehouse_id, client_application, statement_type, execution_status, error_message,
         start_time, end_time, total_duration_ms, compilation_duration_ms, execution_duration_ms,
         waiting_for_compute_duration_ms, waiting_at_capacity_duration_ms, result_fetch_duration_ms, total_task_duration_ms,
         read_rows, produced_rows, read_bytes, from_result_cache, spilled_local_bytes
  FROM system.query.history
  WHERE start_time >= date_sub(current_date(), {LB}) AND workspace_id IN ({WS})
    AND query_source.genie_space_id IS NOT NULL
)
SELECT q.*, s.title AS space_title,
       CAST(q.start_time AS DATE) AS query_date,
       q.total_task_duration_ms / NULLIF(w.task_ms, 0) * c.dbus AS attributed_dbus,
       q.total_task_duration_ms / NULLIF(w.task_ms, 0) * c.usd  AS attributed_cost_usd
FROM q
LEFT JOIN {FQ}.genie_spaces s ON q.space_id = s.space_id AND q.workspace_id = s.workspace_id
LEFT JOIN wh_work w ON q.workspace_id = w.workspace_id AND q.warehouse_id = w.warehouse_id AND date_trunc('HOUR', q.start_time) = w.hr
LEFT JOIN wh_cost c ON q.workspace_id = c.workspace_id AND q.warehouse_id = c.warehouse_id AND date_trunc('HOUR', q.start_time) = c.hr
""")

# COMMAND ----------

# DBTITLE 1,g_genie_billing — Genie LLM / serving consumption
run("""
CREATE OR REPLACE TABLE {FQ}.g_genie_billing
COMMENT 'Genie product consumption (billing_origin_product = GENIE): surface, channel, space (agent_id), user. List-price USD.'
AS
WITH prices AS (
  SELECT sku_name, cloud, usage_unit, price_start_time, price_end_time,
         COALESCE(pricing.effective_list.default, pricing.default) AS usd_per_unit
  FROM system.billing.list_prices WHERE currency_code = 'USD'
)
SELECT u.workspace_id, u.usage_date, u.sku_name,
       u.usage_metadata.genie.surface AS surface,
       u.usage_metadata.genie.channel AS channel,
       u.usage_metadata.genie.agent_id AS space_id,
       s.title AS space_title,
       u.identity_metadata.run_as AS run_as,
       u.sku_name LIKE '%FREE%' AS is_free_tier,
       SUM(u.usage_quantity) AS dbus,
       SUM(u.usage_quantity * COALESCE(p.usd_per_unit, 0)) AS list_cost_usd,
       SUM(CASE WHEN u.sku_name LIKE '%FREE%' THEN 0 ELSE u.usage_quantity * COALESCE(p.usd_per_unit, 0) END) AS billable_list_cost_usd
FROM system.billing.usage u
LEFT JOIN prices p ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
  AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
LEFT JOIN {FQ}.genie_spaces s ON u.usage_metadata.genie.agent_id = s.space_id AND u.workspace_id = s.workspace_id
WHERE u.billing_origin_product = 'GENIE'
  AND u.usage_date >= date_sub(current_date(), {LB}) AND u.workspace_id IN ({WS})
GROUP BY ALL
""")

# COMMAND ----------

# DBTITLE 1,g_messages — one row per Genie question, fully enriched
run("""
CREATE OR REPLACE TABLE {FQ}.g_messages
COMMENT 'One row per Genie message: outcome, latency split (LLM vs SQL), SQL stats, attributed cost, feedback (per-rater latest ratings), user email'
AS
WITH base AS (
WITH stmt AS (
  SELECT m.message_id, m.workspace_id,
         COUNT(q.statement_id) AS n_statements,
         SUM(q.total_duration_ms) / 1000.0 AS sql_total_sec,
         SUM(q.execution_duration_ms) / 1000.0 AS sql_exec_sec,
         SUM(q.waiting_for_compute_duration_ms + q.waiting_at_capacity_duration_ms) / 1000.0 AS sql_queue_sec,
         SUM(q.read_rows) AS sql_read_rows, SUM(q.read_bytes) AS sql_read_bytes,
         MAX(CASE WHEN q.execution_status = 'FAILED' THEN 1 ELSE 0 END) = 1 AS sql_failed,
         MAX(q.error_message) AS sql_error_message,
         SUM(q.attributed_cost_usd) AS sql_cost_usd,
         MAX(q.executed_by) AS sql_executed_by
  FROM (SELECT workspace_id, message_id, explode(statement_ids) AS sid FROM {FQ}.genie_messages) m
  JOIN {FQ}.g_query_stats q ON q.statement_id = m.sid AND q.workspace_id = m.workspace_id
  GROUP BY 1,2
),
rater_fb AS (   -- latest rating per rater per message (REMOVED = rating withdrawn)
  SELECT message_id, user_email, MAX_BY(feedback_rating, event_time) AS rating
  FROM {FQ}.g_audit_events WHERE event_category = 'FEEDBACK' AND message_id IS NOT NULL GROUP BY 1, 2
),
audit_fb AS (
  SELECT message_id, COUNT_IF(rating = 'POSITIVE') AS up, COUNT_IF(rating = 'NEGATIVE') AS down
  FROM rater_fb GROUP BY 1
),
reviews AS (
  SELECT message_id, COUNT(*) AS n_review_requests FROM {FQ}.g_audit_events
  WHERE event_category = 'REVIEW_REQUEST' AND message_id IS NOT NULL GROUP BY 1
)
SELECT m.workspace_id, m.space_id, s.title AS space_title, m.conversation_id, c.title AS conversation_title,
       m.message_id, m.user_id,
       COALESCE(u.user_name, concat('user_id:', m.user_id)) AS user_email, u.display_name AS user_display_name,
       COALESCE(u.principal_type, 'UNKNOWN') AS principal_type,
       COALESCE(m.agent_type, c.agent_type) AS agent_type,
       m.created_ts, CAST(m.created_ts AS DATE) AS created_date,
       hour(m.created_ts) AS hour_of_day, date_format(m.created_ts, 'E') AS day_of_week,
       m.message_position, m.message_position > 1 AS is_follow_up,
       m.question, m.status,
       CASE WHEN m.status = 'FAILED' THEN 'Failed'
            WHEN m.status = 'CANCELLED' THEN 'Cancelled'
            WHEN m.status <> 'COMPLETED' THEN 'In progress / other'
            WHEN m.is_clarification THEN 'Asked clarification'
            WHEN m.n_query_attachments > 0 AND COALESCE(st.sql_failed, false) THEN 'SQL error'
            WHEN m.n_query_attachments > 0 THEN 'Answered with SQL'
            WHEN m.has_text THEN 'Text-only answer'
            ELSE 'No answer' END AS outcome,
       m.status = 'COMPLETED' AS is_completed,
       m.response_time_sec,
       st.sql_total_sec, st.sql_exec_sec, st.sql_queue_sec,
       GREATEST(m.response_time_sec - COALESCE(st.sql_total_sec, 0), 0) AS genie_reasoning_sec,
       CASE WHEN m.response_time_sec IS NULL THEN NULL
            WHEN m.response_time_sec < 10 THEN '1: <10s' WHEN m.response_time_sec < 30 THEN '2: 10-30s'
            WHEN m.response_time_sec < 60 THEN '3: 30-60s' WHEN m.response_time_sec < 120 THEN '4: 1-2 min'
            ELSE '5: >2 min' END AS latency_bucket,
       m.sql, m.sql_title, m.sql_description, m.statement_ids, COALESCE(st.n_statements, 0) AS n_statements,
       m.row_count, m.is_truncated, m.text_response, m.text_purpose, m.is_clarification,
       m.has_viz, m.has_suggested_questions, m.auto_regenerate_count, m.thought_count, m.data_sources,
       m.error_type, COALESCE(m.error_message, st.sql_error_message) AS error_message,
       st.sql_read_rows, st.sql_read_bytes, COALESCE(st.sql_cost_usd, 0) AS sql_cost_usd,
       -- audit ratings (one per rater) win; the API rating is used only when no audit rating exists
       CASE WHEN af.message_id IS NOT NULL THEN af.up
            WHEN m.feedback_rating RLIKE 'POSITIVE|THUMBS_UP$' THEN 1 ELSE 0 END AS thumbs_up,
       CASE WHEN af.message_id IS NOT NULL THEN af.down
            WHEN m.feedback_rating RLIKE 'NEGATIVE|THUMBS_DOWN$' THEN 1 ELSE 0 END AS thumbs_down,
       COALESCE(r.n_review_requests, 0) AS n_review_requests,
       m.feedback_rating AS api_feedback_rating
FROM {FQ}.genie_messages m
LEFT JOIN {FQ}.genie_spaces s ON m.space_id = s.space_id AND m.workspace_id = s.workspace_id
LEFT JOIN {FQ}.genie_conversations c ON m.conversation_id = c.conversation_id AND m.workspace_id = c.workspace_id
LEFT JOIN {FQ}.genie_users u ON m.user_id = u.user_id
LEFT JOIN stmt st ON m.message_id = st.message_id AND m.workspace_id = st.workspace_id
LEFT JOIN audit_fb af ON m.message_id = af.message_id
LEFT JOIN reviews r ON m.message_id = r.message_id
)
SELECT *,
       CASE WHEN thumbs_up > 0 AND thumbs_down > 0 THEN 'MIXED' WHEN thumbs_up > 0 THEN 'POSITIVE'
            WHEN thumbs_down > 0 THEN 'NEGATIVE' END AS feedback_rating,
       CASE WHEN thumbs_up + thumbs_down > 0 THEN 1 ELSE 0 END AS is_rated
FROM base
""")

# COMMAND ----------

# DBTITLE 1,g_feedback — one row per rated answer (latest rating per rater)
run("""
CREATE OR REPLACE TABLE {FQ}.g_feedback
COMMENT 'Thumbs up/down feedback: latest rating per rater × message (Genie spaces) or × conversation (Genie One), joined to the rated answer'
AS
WITH ev AS (
  SELECT *,
         CASE WHEN service_name = 'genieChat' THEN 'Genie One' ELSE 'Genie space' END AS surface,
         COALESCE(message_id, response_id, concat('conv:', conversation_id)) AS rated_item
  FROM {FQ}.g_audit_events WHERE event_category IN ('FEEDBACK','GENIE_ONE_FEEDBACK')
),
latest AS (
  SELECT workspace_id, surface, rated_item, user_email AS rater_email,
         MAX_BY(feedback_rating, event_time) AS rating,
         MAX_BY(channel, event_time) AS channel,
         MAX(event_time) AS feedback_ts, MIN(event_time) AS first_feedback_ts,
         COUNT(*) AS n_feedback_events,
         COUNT(DISTINCT feedback_rating) > 1 AS rating_changed,
         MAX(space_id) AS space_id, MAX(conversation_id) AS conversation_id, MAX(message_id) AS message_id
  FROM ev GROUP BY 1,2,3,4
),
api_only AS (   -- ratings visible via the API but with no audit event in the lookback window
  SELECT m.workspace_id, 'Genie space' AS surface, m.message_id AS rated_item, CAST(NULL AS STRING) AS rater_email,
         CASE WHEN m.api_feedback_rating RLIKE 'POSITIVE|THUMBS_UP$' THEN 'POSITIVE'
              WHEN m.api_feedback_rating RLIKE 'NEGATIVE|THUMBS_DOWN$' THEN 'NEGATIVE' END AS rating,
         'API harvest' AS channel, CAST(NULL AS TIMESTAMP) AS feedback_ts, CAST(NULL AS TIMESTAMP) AS first_feedback_ts,
         1 AS n_feedback_events, false AS rating_changed, m.space_id, m.conversation_id, m.message_id
  FROM {FQ}.g_messages m
  WHERE m.api_feedback_rating IS NOT NULL AND NOT EXISTS (SELECT 1 FROM latest l WHERE l.message_id = m.message_id)
),
f AS (SELECT * FROM latest UNION ALL SELECT * FROM api_only)
SELECT f.workspace_id, f.surface, f.channel, f.rater_email, f.rating,
       CASE f.rating WHEN 'POSITIVE' THEN 1 ELSE 0 END AS thumbs_up,
       CASE f.rating WHEN 'NEGATIVE' THEN 1 ELSE 0 END AS thumbs_down,
       f.feedback_ts, CAST(COALESCE(f.feedback_ts, m.created_ts) AS DATE) AS feedback_date,
       f.n_feedback_events, f.rating_changed,
       f.space_id, COALESCE(m.space_title, s.title, CASE WHEN f.surface = 'Genie One' THEN 'Genie One (no space)' END) AS space_title,
       f.conversation_id, f.message_id,
       m.user_email AS asker_email, m.user_email = f.rater_email AS rated_own_answer,
       m.question, m.outcome, m.response_time_sec, m.latency_bucket,
       CASE WHEN m.is_follow_up THEN 'Follow-up' WHEN m.message_id IS NOT NULL THEN 'New conversation' END AS turn_type,
       m.agent_type, m.row_count, m.sql, m.text_response, m.n_review_requests > 0 AS has_review_request,
       (unix_timestamp(f.first_feedback_ts) - unix_timestamp(m.created_ts)) / 60.0 AS minutes_to_feedback
FROM f
LEFT JOIN {FQ}.g_messages m ON f.message_id = m.message_id AND f.workspace_id = m.workspace_id
LEFT JOIN {FQ}.genie_spaces s ON f.space_id = s.space_id AND f.workspace_id = s.workspace_id
WHERE f.rating IN ('POSITIVE','NEGATIVE')
""")

# COMMAND ----------

# DBTITLE 1,g_table_usage — attached vs actually-used tables
run("""
CREATE OR REPLACE TABLE {FQ}.g_table_usage
COMMENT 'Tables attached to each space and how often Genie actually sourced from them (from message thoughts), last 90 days'
AS
WITH used AS (
  SELECT workspace_id, space_id, lower(ds) AS table_identifier, COUNT(*) AS n_messages_using, MAX(created_ts) AS last_used_ts
  FROM {FQ}.genie_messages LATERAL VIEW explode(data_sources) e AS ds
  WHERE created_ts >= date_sub(current_date(), 90)
  GROUP BY 1,2,3
)
SELECT t.workspace_id, t.space_id, s.title AS space_title, t.table_identifier, t.n_column_configs, s.harvest_scope,
       COALESCE(u.n_messages_using, 0) AS n_messages_using_90d, u.last_used_ts,
       -- only meaningful when every user's messages were harvested
       CASE WHEN s.harvest_scope = 'ALL_USERS' THEN COALESCE(u.n_messages_using, 0) = 0 END AS is_unused_90d
FROM {FQ}.genie_space_tables t
JOIN {FQ}.genie_spaces s ON t.space_id = s.space_id AND t.workspace_id = s.workspace_id
LEFT JOIN used u ON t.space_id = u.space_id AND t.workspace_id = u.workspace_id AND lower(t.table_identifier) = u.table_identifier
""")

# COMMAND ----------

# DBTITLE 1,g_eval_runs — benchmark accuracy trend
run("""
CREATE OR REPLACE TABLE {FQ}.g_eval_runs
COMMENT 'Genie benchmark evaluation runs with accuracy'
AS
SELECT r.*, s.title AS space_title, CAST(r.created_ts AS DATE) AS run_date, u.user_name AS run_by,
       ROW_NUMBER() OVER (PARTITION BY r.workspace_id, r.space_id ORDER BY r.created_ts DESC) = 1 AS is_latest
FROM {FQ}.genie_eval_runs r
LEFT JOIN {FQ}.genie_spaces s ON r.space_id = s.space_id AND r.workspace_id = s.workspace_id
LEFT JOIN {FQ}.genie_users u ON r.run_by_user_id = u.user_id
WHERE r.status = 'DONE'
""")

# COMMAND ----------

# DBTITLE 1,g_space_scorecard — one row per space
run("""
CREATE OR REPLACE TABLE {FQ}.g_space_scorecard
COMMENT 'Per-space scorecard: configuration maturity, reach vs adoption, quality, benchmark accuracy, cost and health flags (last 30 days)'
AS
WITH live AS (
  SELECT workspace_id, explode(live_space_ids) AS space_id FROM (
    SELECT workspace_id, MAX_BY(live_space_ids, run_ts) AS live_space_ids FROM {FQ}.genie_harvest_log WHERE NOT space_filter GROUP BY 1)
),
acl AS (
  SELECT workspace_id, space_id,
         COUNT(DISTINCT CASE WHEN principal_type = 'USER' THEN principal END) AS n_users_with_access,
         COUNT(DISTINCT CASE WHEN principal_type = 'GROUP' THEN principal END) AS n_groups_with_access,
         COUNT(DISTINCT CASE WHEN permission_level IN ('CAN_MANAGE','CAN_EDIT') THEN principal END) AS n_editors,
         MAX(CASE WHEN principal_type = 'GROUP' AND principal IN ('users','account users') THEN 1 ELSE 0 END) = 1 AS shared_with_all_users
  FROM {FQ}.genie_space_acl GROUP BY 1,2
),
aud AS (
  SELECT workspace_id, space_id,
         COUNT_IF(event_category IN ('QUESTION','AGENT_MODE_QUESTION')) AS questions_30d,
         COUNT(DISTINCT CASE WHEN event_category IN ('QUESTION','AGENT_MODE_QUESTION') THEN user_email END) AS active_users_30d,
         COUNT(DISTINCT CASE WHEN event_category IN ('QUESTION','AGENT_MODE_QUESTION') AND event_date >= date_sub(current_date(), 7) THEN user_email END) AS active_users_7d,
         COUNT_IF(event_category = 'REVIEW_REQUEST') AS review_requests_30d,
         COUNT_IF(event_category = 'SPACE_CONFIG_CHANGE') AS config_changes_30d
  FROM {FQ}.g_audit_events WHERE event_date >= date_sub(current_date(), 30) GROUP BY 1,2
),
fb AS (
  SELECT workspace_id, space_id, SUM(thumbs_up) AS thumbs_up_30d, SUM(thumbs_down) AS thumbs_down_30d,
         COUNT(DISTINCT rater_email) AS feedback_givers_30d
  FROM {FQ}.g_feedback WHERE feedback_date >= date_sub(current_date(), 30) AND surface = 'Genie space' GROUP BY 1,2
),
last_use AS (
  SELECT workspace_id, space_id,
         MAX(event_date) FILTER (WHERE event_category IN ('QUESTION','AGENT_MODE_QUESTION')) AS last_question_date,
         MIN_BY(user_email, event_time) FILTER (WHERE event_category IN ('SPACE_CREATE','SPACE_CLONE')) AS creator_email,
         MAX(cloned_from_space_id) AS cloned_from_space_id
  FROM {FQ}.g_audit_events GROUP BY 1,2
),
msg AS (
  SELECT workspace_id, space_id,
         COUNT(*) AS api_messages_30d,
         AVG(CASE WHEN is_completed THEN 1.0 ELSE 0.0 END) AS completion_rate_30d,
         AVG(CASE WHEN outcome = 'Answered with SQL' THEN 1.0 ELSE 0.0 END) AS sql_answer_rate_30d,
         AVG(CASE WHEN is_clarification THEN 1.0 ELSE 0.0 END) AS clarification_rate_30d,
         AVG(CASE WHEN is_follow_up THEN 1.0 ELSE 0.0 END) AS follow_up_rate_30d,
         percentile_approx(response_time_sec, 0.5) AS p50_response_sec_30d,
         percentile_approx(response_time_sec, 0.9) AS p90_response_sec_30d,
         SUM(sql_cost_usd) AS sql_cost_usd_30d
  FROM {FQ}.g_messages WHERE created_ts >= date_sub(current_date(), 30) GROUP BY 1,2
),
qcost AS (
  SELECT workspace_id, space_id, SUM(attributed_cost_usd) AS warehouse_cost_usd_30d, COUNT(*) AS sql_statements_30d,
         AVG(CASE WHEN execution_status = 'FAILED' THEN 1.0 ELSE 0.0 END) AS sql_failure_rate_30d
  FROM {FQ}.g_query_stats WHERE start_time >= date_sub(current_date(), 30) GROUP BY 1,2
),
llm AS (
  SELECT workspace_id, space_id, SUM(list_cost_usd) AS genie_llm_cost_usd_30d, SUM(dbus) AS genie_llm_dbus_30d
  FROM {FQ}.g_genie_billing WHERE usage_date >= date_sub(current_date(), 30) AND space_id IS NOT NULL GROUP BY 1,2
),
ev AS (
  SELECT workspace_id, space_id, accuracy AS latest_benchmark_accuracy, run_date AS latest_benchmark_date
  FROM {FQ}.g_eval_runs WHERE is_latest
),
tu AS (
  SELECT workspace_id, space_id, COUNT_IF(is_unused_90d) AS n_unused_tables_90d FROM {FQ}.g_table_usage GROUP BY 1,2
),
base AS (
  SELECT s.workspace_id, s.space_id, s.title AS space_title, s.description, s.warehouse_id, s.parent_path, s.created_ts, s.updated_ts,
         s.harvest_scope, (l.space_id IS NOT NULL) AS is_active, lu.creator_email, lu.cloned_from_space_id,
         s.n_tables, s.n_column_configs, s.n_entity_matching_columns, s.n_sample_questions, s.n_text_instructions, s.instruction_chars,
         s.n_example_sqls, s.n_sql_functions, s.n_join_specs, s.n_sql_snippets, s.n_benchmark_questions, COALESCE(tu.n_unused_tables_90d, 0) AS n_unused_tables_90d,
         COALESCE(acl.n_users_with_access, 0) AS n_users_with_access, COALESCE(acl.n_groups_with_access, 0) AS n_groups_with_access,
         COALESCE(acl.n_editors, 0) AS n_editors, COALESCE(acl.shared_with_all_users, false) AS shared_with_all_users,
         COALESCE(aud.questions_30d, 0) AS questions_30d, COALESCE(aud.active_users_30d, 0) AS active_users_30d,
         COALESCE(aud.active_users_7d, 0) AS active_users_7d,
         COALESCE(fb.thumbs_up_30d, 0) AS thumbs_up_30d, COALESCE(fb.thumbs_down_30d, 0) AS thumbs_down_30d,
         COALESCE(fb.feedback_givers_30d, 0) AS feedback_givers_30d,
         COALESCE(aud.review_requests_30d, 0) AS review_requests_30d, COALESCE(aud.config_changes_30d, 0) AS config_changes_30d,
         lu.last_question_date, datediff(current_date(), lu.last_question_date) AS days_since_last_question,
         msg.api_messages_30d, msg.completion_rate_30d, msg.sql_answer_rate_30d, msg.clarification_rate_30d, msg.follow_up_rate_30d,
         msg.p50_response_sec_30d, msg.p90_response_sec_30d,
         qcost.sql_statements_30d, qcost.sql_failure_rate_30d,
         COALESCE(qcost.warehouse_cost_usd_30d, 0) AS warehouse_cost_usd_30d,
         COALESCE(llm.genie_llm_cost_usd_30d, 0) AS genie_llm_cost_usd_30d,
         ev.latest_benchmark_accuracy, ev.latest_benchmark_date
  FROM {FQ}.genie_spaces s
  LEFT JOIN live l ON s.space_id = l.space_id AND s.workspace_id = l.workspace_id
  LEFT JOIN acl ON s.space_id = acl.space_id AND s.workspace_id = acl.workspace_id
  LEFT JOIN aud ON s.space_id = aud.space_id AND s.workspace_id = aud.workspace_id
  LEFT JOIN fb ON s.space_id = fb.space_id AND s.workspace_id = fb.workspace_id
  LEFT JOIN last_use lu ON s.space_id = lu.space_id AND s.workspace_id = lu.workspace_id
  LEFT JOIN msg ON s.space_id = msg.space_id AND s.workspace_id = msg.workspace_id
  LEFT JOIN qcost ON s.space_id = qcost.space_id AND s.workspace_id = qcost.workspace_id
  LEFT JOIN llm ON s.space_id = llm.space_id AND s.workspace_id = llm.workspace_id
  LEFT JOIN ev ON s.space_id = ev.space_id AND s.workspace_id = ev.workspace_id
  LEFT JOIN tu ON s.space_id = tu.space_id AND s.workspace_id = tu.workspace_id
)
SELECT *,
  -- Configuration maturity 0-100 (Genie best practices: instructions, example SQL, joins, sample questions, benchmarks, focused table set)
  CAST(  LEAST(n_text_instructions, 1) * 20
       + LEAST(n_example_sqls, 5) * 4
       + CASE WHEN n_tables <= 1 OR n_join_specs > 0 THEN 10 ELSE 0 END
       + LEAST(n_sample_questions, 3) * 10 / 3
       + LEAST(n_benchmark_questions, 10) * 2
       + CASE WHEN n_tables BETWEEN 1 AND 25 THEN 10 ELSE 0 END
       + CASE WHEN n_column_configs > 0 THEN 10 ELSE 0 END AS INT) AS config_score,
  (thumbs_up_30d + thumbs_down_30d) AS feedback_30d,
  thumbs_up_30d / NULLIF(thumbs_up_30d + thumbs_down_30d, 0) AS positive_feedback_rate_30d,
  (thumbs_up_30d + thumbs_down_30d) / NULLIF(questions_30d, 0) AS feedback_coverage_30d,
  -- group grants are not expanded, so the denominator is only known when access is granted to users directly
  CASE WHEN n_groups_with_access = 0 THEN active_users_30d / NULLIF(n_users_with_access, 0) END AS adoption_rate_30d,
  warehouse_cost_usd_30d + genie_llm_cost_usd_30d AS total_cost_usd_30d,
  (warehouse_cost_usd_30d + genie_llm_cost_usd_30d) / NULLIF(questions_30d, 0) AS cost_per_question_usd_30d,
  concat_ws(' · ',
    CASE WHEN NOT is_active THEN 'Trashed / no access' END,
    CASE WHEN is_active AND COALESCE(days_since_last_question, 999) > 30 THEN 'Unused 30d' END,
    CASE WHEN n_text_instructions = 0 THEN 'No instructions' END,
    CASE WHEN n_example_sqls = 0 THEN 'No example SQL' END,
    CASE WHEN n_benchmark_questions = 0 THEN 'No benchmarks' END,
    CASE WHEN n_tables > 25 THEN 'Too many tables' END,
    CASE WHEN n_unused_tables_90d > 0 AND api_messages_30d > 20 AND harvest_scope = 'ALL_USERS' THEN 'Unused tables attached' END,
    CASE WHEN latest_benchmark_accuracy < 0.8 THEN 'Benchmark < 80%' END,
    CASE WHEN completion_rate_30d < 0.9 AND api_messages_30d >= 10 THEN 'Low completion' END,
    CASE WHEN thumbs_down_30d > thumbs_up_30d AND thumbs_down_30d >= 3 THEN 'Mostly negative feedback' END,
    CASE WHEN harvest_scope = 'CALLER_ONLY' THEN 'Partial harvest (no CAN MANAGE)' END
  ) AS health_flags
FROM base
""")

# COMMAND ----------

for t in ["g_audit_events", "g_query_stats", "g_genie_billing", "g_messages", "g_feedback", "g_table_usage", "g_eval_runs", "g_space_scorecard"]:
    print(t, spark.table(f"{CATALOG}.{SCHEMA}.{t}").count())
