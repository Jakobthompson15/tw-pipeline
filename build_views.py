"""Build triple_whale_staging: one typed view per raw table, plus flattened helpers.

  python build_views.py            # (re)create all staging views
  python build_views.py --dry-run  # print the SQL only

Raw rows are JSON (see main.py). Each staging view exposes every top-level key as
its own column, typed from a profile of the data:
  number            -> FLOAT64 (ids -> STRING, event_hour -> INT64)
  boolean           -> BOOL
  "YYYY-MM-DD"      -> DATE
  "YYYY-MM-DD HH:.."-> TIMESTAMP (strings without an offset are treated as UTC)
  other strings     -> STRING
  arrays / objects  -> JSON (flatten downstream)
Re-run after Triple Whale adds fields; the views are cheap to replace.
"""

import argparse
import os
import re
from concurrent.futures import ThreadPoolExecutor

from google.cloud import bigquery

PROJECT = os.environ.get("PROJECT", "bgzdata")
RAW, STG = "triple_whale_raw", "triple_whale_staging"
SAMPLE_ROWS = 20_000

ID_COL = re.compile(r"(^|_)(id|ids)$|_key$")
INT_COLS = {"event_hour", "hour"}
TEXTY = re.compile(r"(name|title|url|status|type|text|copy|code|tags?|date|_at|email|subject|country|city|state|currency)s?$")
DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
TS_RE = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}"


def view_name(table):
    return re.sub(r"_(table|tvf)$", "", table)


def col_name(key, taken):
    name = re.sub(r"[^A-Za-z0-9_]", "_", key).lower() or "field"
    if name[0].isdigit():
        name = "f_" + name
    base, i = name, 2
    while name in taken:
        name, i = f"{base}_{i}", i + 1
    taken.add(name)
    return name


def profile(bq, table):
    sql = f"""
      SELECT k,
        COUNTIF(t = 'number') AS num, COUNTIF(t = 'boolean') AS bool,
        COUNTIF(t = 'string') AS str, COUNTIF(t IN ('array', 'object')) AS nested,
        COUNTIF(t = 'number' AND LAX_FLOAT64(v) != TRUNC(LAX_FLOAT64(v))) AS frac,
        COUNTIF(t = 'string' AND REGEXP_CONTAINS(JSON_VALUE(v), r'{DATE_RE}')) AS dates,
        COUNTIF(t = 'string' AND REGEXP_CONTAINS(JSON_VALUE(v), r'{TS_RE}')) AS ts,
        COUNTIF(t = 'string' AND JSON_VALUE(v) != '') AS nonempty
      FROM (
        SELECT k, data[k] AS v, JSON_TYPE(data[k]) AS t
        FROM (SELECT data FROM `{PROJECT}.{RAW}.{table}` LIMIT {SAMPLE_ROWS}),
             UNNEST(JSON_KEYS(data, 1)) AS k)
      GROUP BY k ORDER BY k"""
    return list(bq.query(sql).result())


def column_expr(p):
    """(sql_type, expression) for one profiled key."""
    acc = f"data[{quote(p.k)}]"
    typed = p.num + p.bool + p.str + p.nested
    if p.nested and p.nested == typed:
        return "JSON", acc
    if ID_COL.search(p.k):
        return "STRING", f"LAX_STRING({acc})"
    if typed == 0:  # only nulls in the sample: guess from the name
        if TEXTY.search(p.k):
            return "STRING", f"LAX_STRING({acc})"
        return "FLOAT64", f"LAX_FLOAT64({acc})"
    if ID_COL.search(p.k):
        return "STRING", f"LAX_STRING({acc})"
    if p.num == typed:
        if p.k in INT_COLS and not p.frac:
            return "INT64", f"LAX_INT64({acc})"
        return "FLOAT64", f"LAX_FLOAT64({acc})"
    if p.bool == typed:
        return "BOOL", f"LAX_BOOL({acc})"
    if p.str == typed and p.nonempty:
        if p.dates == p.nonempty:
            return "DATE", f"SAFE_CAST(NULLIF(JSON_VALUE({acc}), '') AS DATE)"
        if p.ts == p.nonempty:
            return "TIMESTAMP", f"SAFE_CAST(NULLIF(JSON_VALUE({acc}), '') AS TIMESTAMP)"
    if p.nested:
        return "JSON", acc
    return "STRING", f"LAX_STRING({acc})"


def quote(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def generic_view(bq, table):
    taken = {"shop_id", "event_date", "_ingested_at", "_run_id"}
    cols = []
    for p in profile(bq, table):
        if p.k in ("shop_id", "event_date"):
            continue
        typ, expr = column_expr(p)
        cols.append(f"  {expr} AS {col_name(p.k, taken)},  -- {typ}")
    body = "\n".join(cols)
    return f"""SELECT
  shop_id,
  event_date,
{body}
  _ingested_at
FROM `{PROJECT}.{RAW}.{table}`"""


# Hand-written views on top of the generic ones, for nested structures.
EXTRA_VIEWS = {
    # Summary Page: one row per metric per day with the value pulled out.
    "summary_page_metric_values": f"""
SELECT
  shop_id,
  event_date,
  LAX_STRING(data.metricId) AS metric_id,
  LAX_STRING(data.id) AS metric_key,
  LAX_STRING(data.title) AS title,
  LAX_STRING(data.type) AS value_type,
  LAX_FLOAT64(data.`values`.current) AS value,
  ARRAY(SELECT LAX_STRING(s) FROM UNNEST(JSON_QUERY_ARRAY(data.services)) s) AS services,
  _ingested_at
FROM `{PROJECT}.{RAW}.summary_page_metrics`""",

    # One row per touchpoint in each order's journey.
    "order_touchpoints": f"""
SELECT
  o.shop_id,
  o.event_date AS order_date,
  LAX_STRING(o.data.order_id) AS order_id,
  pos + 1 AS touch_number,
  LAX_STRING(t.event) AS event,
  LAX_STRING(t.path) AS path,
  LAX_STRING(t.productId) AS product_id,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', LAX_STRING(t.time)) AS event_time,
  t AS raw
FROM `{PROJECT}.{RAW}.orders_with_journeys` o,
  UNNEST(JSON_QUERY_ARRAY(o.data.journey)) t WITH OFFSET pos""",

    # One row per order x attribution model x credited click. Triple Whale sends no
    # weights, so credit splits the order evenly across the model's clicks
    # (1 for single-click models, 1/n for linear).
    "order_attribution": f"""
SELECT
  *,
  1 / COUNT(*) OVER (PARTITION BY order_id, model) AS credit,
  total_price / COUNT(*) OVER (PARTITION BY order_id, model) AS attributed_revenue
FROM (
  SELECT
    o.shop_id,
    o.event_date AS order_date,
    LAX_STRING(o.data.order_id) AS order_id,
    LAX_STRING(o.data.order_name) AS order_name,
    LAX_FLOAT64(o.data.total_price) AS total_price,
    model,
    pos + 1 AS click_number,
    LAX_STRING(c.source) AS source,
    LAX_STRING(c.campaignId) AS campaign_id,
    LAX_STRING(c.adsetId) AS adset_id,
    LAX_STRING(c.adId) AS ad_id,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', LAX_STRING(c.clickDate)) AS click_time
  FROM `{PROJECT}.{RAW}.orders_with_journeys` o,
    UNNEST(['firstClick', 'lastClick', 'fullFirstClick', 'fullLastClick',
            'lastPlatformClick', 'linear', 'linearAll']) AS model,
    UNNEST(JSON_QUERY_ARRAY(o.data.attribution[model])) c WITH OFFSET pos)""",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="", help="comma-separated view names")
    args = ap.parse_args()
    bq = bigquery.Client(project=PROJECT)
    only = set(filter(None, args.only.split(",")))
    tables = sorted(t.table_id for t in bq.list_tables(f"{PROJECT}.{RAW}")
                    if not t.table_id.startswith("_"))
    jobs = {view_name(t): (lambda t=t: generic_view(bq, t)) for t in tables}
    jobs.update({name: (lambda sql=sql: sql) for name, sql in EXTRA_VIEWS.items()})
    jobs["sync_runs"] = lambda: f"SELECT * FROM `{PROJECT}.{RAW}._sync_runs`"

    def build(name):
        ddl = f"CREATE OR REPLACE VIEW `{PROJECT}.{STG}.{name}` AS\n{jobs[name]()}"
        if args.dry_run:
            return ddl + ";\n"
        bq.query(ddl).result()
        return f"view {STG}.{name}"

    names = [n for n in jobs if not only or n in only]
    with ThreadPoolExecutor(8) as ex:
        for out in ex.map(build, names):
            print(out, flush=True)

if __name__ == "__main__":
    main()
