"""Triple Whale -> BigQuery raw sync for BGZ (BodyGuardz).

Runs as a Cloud Run job under the tw-pipeline service account.

  python main.py                 # daily: re-pull the last LOOKBACK_DAYS days + refresh snapshots
  python main.py --backfill      # full history for every source
  python main.py --only orders_table,reviews_table   # limit to some sources (for testing)

Every Triple Whale table lands in triple_whale_raw.<name> with the same shape:
  shop_id STRING, event_date DATE, data JSON, _ingested_at TIMESTAMP, _run_id STRING
Rows are stored as JSON so upstream schema changes never break the load; typed
columns belong in triple_whale_staging.

Dated tables are replaced window by window (DELETE range + INSERT in one
transaction), so re-running any range is idempotent. Snapshot tables (no
event_date) are fully replaced on every run, except customers_table: full on
Sundays, otherwise an upsert of customers created or ordering in the lookback.
"""

import argparse
import datetime as dt
import http.client
import json
import logging
import math
import os
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

from google.cloud import bigquery, secretmanager

PROJECT = os.environ.get("GCP_PROJECT", "bgzdata")
DATASET = os.environ.get("BQ_DATASET", "triple_whale_raw")
SHOP = os.environ.get("TW_SHOP", "xjfdku-0t.myshopify.com")
SHOP_TZ = ZoneInfo(os.environ.get("TW_SHOP_TZ", "America/Chicago"))
SECRET = os.environ.get("TW_SECRET", "triple-whale-api-key")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
SQL_WORKERS = int(os.environ.get("SQL_WORKERS", "4"))
API = "https://api.triplewhale.com/api/v2/"

# Direct contact PII is dropped at the source (never leaves Triple Whale).
# Triple Whale already masks emails; this removes the rest.
PII_COLUMNS = {
    "phone", "phone_number", "first_name", "last_name", "display_name",
    "address1", "address2", "zip", "latitude", "longitude", "note",
    "email", "email_address", "customer_email", "customer_first_name",
    "customer_last_name", "shipping_zip", "shipping_city", "shipping_state",
    "shipping_state_code", "emails", "primary_email",
}

# Tables with an event_date column: pulled by date window.
# first_date is only a fallback when min(event_date) cannot be computed.
DATED = {
    "orders_table": {},
    "refunds_table": {},
    "pixel_orders_table": {},
    "pixel_joined_tvf": {},
    "pixel_keywords_joined_tvf": {},
    "blended_stats_tvf": {},
    "product_analytics_tvf": {},
    "ads_table": {},
    "keywords_table": {},
    "search_terms_table": {},
    "creatives_table": {"splits": ["asset_type = 'image'", "asset_type = 'video'", "asset_type = 'copy'"],
                        "first_date": "2023-08-17", "window_days": 31},
    "activities_table": {},
    "sessions_table": {},
    "web_analytics_table": {},
    "ga4_web_analytics_table": {},
    "ga4_sessions_agg_table": {},
    "ga4_product_analytics_table": {},
    "email_sms_table": {},
    "reviews_table": {},
    "custom_spend_table": {},
    "ai_visibility_table": {},
    "affluencer_hub_table": {},
    # Empty for this shop today; kept so data flows automatically if connected later.
    "affiliates_table": {},
    "external_revenue_table": {},
    "pixel_custom_conversions_table": {},
    "pixel_custom_events_table": {},
    "post_purchase_survey_table": {},
    "subscription_metrics_table": {},
    "shipping_table": {},
    "cs_agg_metrics_table": {},
    "cs_events_table": {},
    "cs_messages_table": {},
    "cs_tickets_table": {},
    "social_media_brand_mentions_table": {},
    "social_media_pages_table": {},
}

# Tables without event_date: full snapshot every run (see INCREMENTAL_SNAPSHOTS).
SNAPSHOT = [
    "customers_table",
    "products_table",
    "id_graph_table",
    "email_sms_settings_table",
    "customer_segmentation_table",
    "ads_experiments_geolift_table",
    "custom_categories_table",
    "custom_category_mappings_table",
    "subscriptions_table",
    "social_media_posts_table",
    "social_media_comments_table",
]
# Not pulled:
#  customer_journey_table - every pageview (450M+ rows, 2-3M/day); order journeys
#    come from the attribution endpoint instead (orders_with_journeys).
#  customer_segmentation_analytics_tvf - needs per-segment parameters; it is an
#    analysis function over customer_segmentation_table + orders.

TOO_BIG = re.compile(r"Limit for result exceeded|memory limit exceeded|Timeout error", re.I)

log = logging.getLogger("tw")


# --------------------------------------------------------------------------- API

class RateLimiter:
    """Allows `per_min` calls per rolling minute and one call every `min_gap` seconds."""

    def __init__(self, per_min, min_gap):
        self.per_min, self.min_gap = per_min, min_gap
        self.calls, self.lock = [], threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.calls = [t for t in self.calls if now - t < 60]
                gap = self.calls[-1] + self.min_gap - now if self.calls else 0
                full = self.calls[0] + 60 - now if len(self.calls) >= self.per_min else 0
                delay = max(gap, full)
                if delay <= 0:
                    self.calls.append(now)
                    return
            time.sleep(delay)


class TooBig(Exception):
    pass


class TripleWhale:
    # Observed limits: SQL 5/s + 100/min, journeys 10/s + 60/min. Stay under both.
    def __init__(self, key):
        self.key = key
        self.limits = {
            "orcabase/api/sql": RateLimiter(90, 0.25),
            "attribution/get-orders-with-journeys-v2": RateLimiter(55, 0.15),
            "summary-page/get-data": RateLimiter(55, 0.5),
        }

    def post(self, path, body, tries=8):
        for attempt in range(tries):
            self.limits[path].wait()
            req = urllib.request.Request(
                API + path, json.dumps(body).encode(),
                {"x-api-key": self.key, "content-type": "application/json"})
            try:
                t0 = time.monotonic()
                with urllib.request.urlopen(req, timeout=600) as r:
                    out = json.loads(r.read())
                took = time.monotonic() - t0
                log.log(logging.INFO if took > 30 else logging.DEBUG, "%s %.1fs %s",
                        path, took, str(body.get("query") or body)[:160])
                return out
            except urllib.error.HTTPError as e:
                text = e.read().decode(errors="replace")[:500]
                if e.code == 400 and TOO_BIG.search(text):
                    raise TooBig(text)
                if e.code in (429, 500, 502, 503, 504):
                    # Triple Whale has sent Retry-After values of ~93000s on the SQL
                    # endpoint while other calls kept succeeding, so never trust it blindly.
                    wait = min(int(e.headers.get("Retry-After") or 0), 120) or min(2 ** attempt, 60)
                    log.warning("%s %s -> %s, retry in %ss: %s", path, attempt, e.code, wait, text[:150])
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"{path} HTTP {e.code}: {text}") from None
            except (OSError, http.client.HTTPException) as e:
                # URLError, timeouts, resets, and responses cut off mid-body
                # (http.client.IncompleteRead) are all transient.
                log.warning("%s network error, retrying: %r", path, e)
                time.sleep(min(2 ** attempt, 60))
        raise RuntimeError(f"{path}: gave up after {tries} attempts")

    def sql(self, query, start="2015-01-01", end="2015-01-01"):
        return self.post("orcabase/api/sql", {
            "shopId": SHOP, "query": query,
            "period": {"startDate": str(start), "endDate": str(end)}})


# ---------------------------------------------------------------------- BigQuery

class Sink:
    """Collects rows for one target range, then swaps them into the raw table."""

    FLUSH_BYTES = 200 * 1024 * 1024

    def __init__(self, bq, table, run_id, dated):
        self.bq, self.table, self.run_id, self.dated = bq, table, run_id, dated
        self.target = f"{PROJECT}.{DATASET}.{table}"
        self.load = f"{PROJECT}.{DATASET}._load_{table}_{run_id[:8]}"
        self.now = dt.datetime.now(dt.timezone.utc).isoformat()
        self.file, self.size, self.rows, self.loaded = None, 0, 0, False

    def add(self, event_date, data, shop_id=SHOP):
        if self.file is None:
            self.file = tempfile.TemporaryFile("w+b")
            self.size = 0
        line = json.dumps({"shop_id": shop_id, "event_date": event_date, "data": data,
                           "_ingested_at": self.now, "_run_id": self.run_id},
                          separators=(",", ":"), default=str).encode() + b"\n"
        self.file.write(line)
        self.size += len(line)
        self.rows += 1
        if self.size >= self.FLUSH_BYTES:
            self._flush()

    def _flush(self):
        if self.file is None:
            return
        self.file.seek(0)
        cfg = bigquery.LoadJobConfig(
            schema=SCHEMA, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND" if self.loaded else "WRITE_TRUNCATE")
        self.bq.load_table_from_file(self.file, self.load, job_config=cfg).result()
        self.file.close()
        self.file, self.loaded = None, True

    def commit(self, start=None, end=None, key=None):
        """Replace [start, end] (dated), the rows sharing `key` with the new ones (upsert),
        or the whole table (snapshot) with the collected rows."""
        self._flush()
        ensure_table(self.bq, self.table, self.dated)
        if key and not self.loaded:
            return 0  # upsert with nothing new: leave the table alone
        if key:
            where = (f"LAX_STRING(data.{key}) IN "
                     f"(SELECT LAX_STRING(data.{key}) FROM `{self.load}`)")
        elif self.dated:
            where = f"event_date BETWEEN DATE '{start}' AND DATE '{end}'"
        else:
            where = "TRUE"
        insert = f"INSERT INTO `{self.target}` SELECT * FROM `{self.load}`;" if self.loaded else ""
        self.bq.query(f"""
            BEGIN TRANSACTION;
            DELETE FROM `{self.target}` WHERE {where};
            {insert}
            COMMIT TRANSACTION;""").result()
        if self.loaded:
            self.bq.delete_table(self.load, not_found_ok=True)
        n, self.rows, self.loaded = self.rows, 0, False
        return n


SCHEMA = [
    bigquery.SchemaField("shop_id", "STRING"),
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("data", "JSON"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_run_id", "STRING"),
]
_ensured, _ensure_lock = set(), threading.Lock()


def ensure_table(bq, table, dated):
    with _ensure_lock:
        if table in _ensured:
            return
        t = bigquery.Table(f"{PROJECT}.{DATASET}.{table}", schema=SCHEMA)
        if dated:
            t.time_partitioning = bigquery.TimePartitioning(field="event_date")
        t.clustering_fields = ["shop_id"]
        t.description = f"Triple Whale {table} (raw JSON rows). Loaded by tw-sync."
        bq.create_table(t, exists_ok=True)
        _ensured.add(table)


# ------------------------------------------------------------------ SQL sources

def scrub(row):
    return {k: v for k, v in row.items() if k not in PII_COLUMNS}


# Columns used to split a big day into hash buckets (first few present in the table win).
# Any deterministic column works; high-cardinality ones balance the buckets.
# Note cityHash64(*) is NOT usable: it returns NULL when any column is NULL.
KEY_CANDIDATES = [
    "order_id", "session_id", "triple_id", "customer_id", "product_id", "variant_id",
    "ad_id", "adset_id", "campaign_id", "keyword_id", "search_term", "message_id",
    "asset_id", "gid", "segment_id", "landing_page", "event_hour", "channel",
]


def key_expr(columns):
    keys = [c for c in KEY_CANDIDATES if c in columns][:4]
    if not keys:
        return None
    return "cityHash64(concat(" + ", '|', ".join(f"ifNull(toString({k}), '')" for k in keys) + "))"


def select(table, extra_where, bucket, dated, key):
    k, i = bucket
    where = ["event_date BETWEEN @startDate AND @endDate"] if dated else ["1 = 1"]
    if extra_where:
        where.append(extra_where)
    if k > 1:
        if not key:
            raise RuntimeError(f"{table}: too big for one call and no key column to split on")
        where.append(f"{key} % {k} = {i}")
    return f"SELECT * FROM {table} WHERE {' AND '.join(where)}"


def fetch(tw, table, start, end, extra_where, bucket, dated, key, emit):
    """Fetch a range (or one hash bucket of it); split in half whenever Triple Whale says it's too big."""
    try:
        rows = tw.sql(select(table, extra_where, bucket, dated, key), start, end)
    except TooBig as e:
        k, i = bucket
        if dated and start < end:
            mid = start + (end - start) // 2
            log.info("%s %s..%s too big, splitting dates", table, start, end)
            fetch(tw, table, start, mid, extra_where, bucket, dated, key, emit)
            fetch(tw, table, mid + dt.timedelta(days=1), end, extra_where, bucket, dated, key, emit)
        elif key and k < 4096:
            log.info("%s %s..%s bucket %s/%s too big, splitting rows", table, start, end, i, k)
            fetch(tw, table, start, end, extra_where, (2 * k, i), dated, key, emit)
            fetch(tw, table, start, end, extra_where, (2 * k, i + k), dated, key, emit)
        else:
            raise RuntimeError(f"{table} cannot be split further: {e}") from None
        return
    for r in rows:
        emit(r)


def sample_table(tw, table, dated):
    """(rows per call, bucket key expression) from a small sample.

    Rows per call is a rough fit for Triple Whale's 10 MiB / 50k-row response limit;
    fetch() splits further if it's still too big. Note count() on aggregated views
    can be lower than the rows SELECT * returns, which fetch() also absorbs.
    """
    where = "event_date BETWEEN @startDate AND @endDate" if dated else "1 = 1"
    today = dt.datetime.now(SHOP_TZ).date()
    try:
        sample = tw.sql(f"SELECT * FROM {table} WHERE {where} LIMIT 200",
                        today - dt.timedelta(days=30), today)
    except (TooBig, RuntimeError):
        sample = None
    if not sample:
        try:
            sample = tw.sql(f"SELECT * FROM {table} LIMIT 1", "2015-01-01", today)
        except (TooBig, RuntimeError):
            sample = []
        return (2000 if sample else 40000), key_expr(sample[0].keys() if sample else [])
    avg = len(json.dumps(sample)) / len(sample)
    return int(max(500, min(40000, 6 * 1024 * 1024 / avg))), key_expr(sample[0].keys())


WINDOW_ROWS = 500_000  # rows collected before each swap into BigQuery


def daily_counts(tw, table, start, end, extra, max_days=None):
    """{date: rows} for [start, end]; splits the range if Triple Whale can't count it in one go."""
    if max_days and (end - start).days + 1 > max_days:
        mid = start + dt.timedelta(days=max_days - 1)
        return {**daily_counts(tw, table, start, mid, extra, max_days),
                **daily_counts(tw, table, mid + dt.timedelta(days=1), end, extra, max_days)}
    where = " AND " + extra if extra else ""
    try:
        counts = tw.sql(f"SELECT toString(event_date) AS d, count() AS n FROM {table} "
                        f"WHERE event_date BETWEEN @startDate AND @endDate{where} GROUP BY d", start, end)
        return {dt.date.fromisoformat(c["d"]): int(c["n"]) for c in counts if int(c["n"])}
    except TooBig:
        if start == end:
            raise
        mid = start + (end - start) // 2
        return {**daily_counts(tw, table, start, mid, extra),
                **daily_counts(tw, table, mid + dt.timedelta(days=1), end, extra)}


def sync_dated(tw, bq, table, cfg, start, end, run_id):
    splits = cfg.get("splits") or [None]
    rpc, key = sample_table(tw, table, True)
    counts = {extra: daily_counts(tw, table, start, end, extra, cfg.get("window_days"))
              for extra in splits}

    # Windows sized by row volume: sparse history goes in one swap, busy periods in several.
    windows, w0, acc, d = [], start, 0, start
    while d <= end:
        acc += sum(c.get(d, 0) for c in counts.values())
        if acc >= WINDOW_ROWS or d == end:
            windows.append((w0, d))
            w0, acc = d + dt.timedelta(days=1), 0
        d += dt.timedelta(days=1)

    sink = Sink(bq, table, run_id, dated=True)
    total = 0
    for w0, w1 in windows:
        for extra in splits:
            for d0, d1, k in plan_chunks(counts[extra], w0, w1, rpc):
                for i in range(k):
                    fetch(tw, table, d0, d1, extra, (k, i), True, key,
                          lambda r: sink.add(r.get("event_date"), scrub(r), r.get("shop_id") or SHOP))
        total += sink.commit(w0, w1)
    log.info("DONE %s %s..%s rows=%s windows=%s", table, start, end, total, len(windows))
    return total


def plan_chunks(per_day, w0, w1, rpc):
    """Group days into calls of <= rpc rows; big days get split into k hash buckets."""
    days = sorted(d for d in per_day if w0 <= d <= w1)
    run_start, run_n = None, 0
    for d in days:
        n = per_day[d]
        if n > rpc:
            if run_start:
                yield run_start, prev, 1
                run_start, run_n = None, 0
            yield d, d, math.ceil(n / rpc)
        else:
            if run_start and run_n + n > rpc:
                yield run_start, prev, 1
                run_start, run_n = None, 0
            run_start = run_start or d
            run_n += n
        prev = d
    if run_start:
        yield run_start, prev, 1


# Snapshots too big to re-pull daily: fully refreshed on FULL_REFRESH_WEEKDAY (and on
# --backfill); other days only rows matching `changed` over the lookback window are
# pulled and upserted on `key`.
INCREMENTAL_SNAPSHOTS = {
    "customers_table": {
        "key": "customer_id",
        "changed": "(toDate(created_at) >= toDate(@startDate) OR customer_id IN "
                   "(SELECT customer_id FROM orders_table "
                   "WHERE event_date BETWEEN @startDate AND @endDate))",
    },
}
FULL_REFRESH_WEEKDAY = int(os.environ.get("FULL_REFRESH_WEEKDAY", "6"))  # Sunday


def sync_snapshot(tw, bq, table, run_id, since=None, until=None):
    """Full snapshot, or, when `since` is given for an incremental table, an upsert of
    the rows changed between since and until."""
    inc = INCREMENTAL_SNAPSHOTS.get(table) if since else None
    extra = inc["changed"] if inc else None
    start, end = (since, until) if inc else (dt.date(2015, 1, 1), dt.date(2015, 1, 1))
    rpc, key = sample_table(tw, table, False)
    try:
        where = f" WHERE {extra}" if extra else ""
        n = int(tw.sql(f"SELECT count() AS n FROM {table}{where}", start, end)[0]["n"])
    except Exception:
        n = rpc
    k = max(1, math.ceil(n / rpc)) if key else 1
    sink = Sink(bq, table, run_id, dated=False)
    for i in range(k):
        fetch(tw, table, start, end, extra, (k, i), False, key,
              lambda r: sink.add(None, scrub(r), r.get("shop_id") or SHOP))
    total = sink.commit(key=inc["key"] if inc else None)
    log.info("DONE %s %s rows=%s", table, f"upsert since {since}" if inc else "snapshot", total)
    return total


def first_date(tw, table, cfg, default):
    """Earliest event_date in the table; `default` when it's empty."""
    try:
        r = tw.sql(f"SELECT toString(min(event_date)) AS d, count() AS n FROM {table} "
                   f"WHERE event_date BETWEEN @startDate AND @endDate", "2015-01-01",
                   dt.datetime.now(SHOP_TZ).date())
        if not r or not int(r[0]["n"]):
            return default
        return dt.date.fromisoformat(r[0]["d"])
    except (TooBig, RuntimeError) as e:
        log.info("%s: min(event_date) failed (%s), using fallback", table, str(e)[:100])
    return dt.date.fromisoformat(cfg.get("first_date", "2023-01-01"))


# ------------------------------------------------------------ journeys + summary

def day_bounds(d):
    a = dt.datetime.combine(d, dt.time.min, SHOP_TZ)
    b = dt.datetime.combine(d, dt.time(23, 59, 59, 999000), SHOP_TZ)
    fmt = lambda x: x.isoformat(timespec="milliseconds")
    return fmt(a), fmt(b)


def journeys_first_date(tw, today):
    """Earliest month with attributed orders (one cheap call per month)."""
    m = dt.date(2019, 1, 1)
    while m <= today:
        nxt = (m.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        a, _ = day_bounds(m)
        _, b = day_bounds(nxt - dt.timedelta(days=1))
        r = tw.post("attribution/get-orders-with-journeys-v2",
                    {"shop": SHOP, "startDate": a, "endDate": b, "page": 1, "pageSize": 1,
                     "excludeJourneyData": True})
        if r.get("totalForRange"):
            return m
        m = nxt
    return today


JOURNEY_WORKERS = int(os.environ.get("JOURNEY_WORKERS", "10"))


def journey_day(tw, d):
    """All orders (with journeys) created on shop-local day d. Pages take ~10s each."""
    a, b = day_bounds(d)
    out, page = [], 1
    while True:
        r = tw.post("attribution/get-orders-with-journeys-v2",
                    {"shop": SHOP, "startDate": a, "endDate": b, "page": page, "pageSize": 100})
        orders = r.get("ordersWithJourneys") or []
        out += orders
        if len(orders) < 100 or len(out) >= (r.get("totalForRange") or 0):
            return out
        page += 1


def sync_journeys(tw, bq, start, end, run_id):
    table = "orders_with_journeys"
    sink = Sink(bq, table, run_id, dated=True)
    total, w0 = 0, start
    with ThreadPoolExecutor(JOURNEY_WORKERS, thread_name_prefix="journey") as ex:
        while w0 <= end:
            w1 = min(w0 + dt.timedelta(days=13), end)
            days = [w0 + dt.timedelta(days=i) for i in range((w1 - w0).days + 1)]
            for d, orders in zip(days, ex.map(lambda d: journey_day(tw, d), days)):
                for o in orders:
                    sink.add(d.isoformat(), o)
            total += sink.commit(w0, w1)
            w0 = w1 + dt.timedelta(days=1)
    log.info("DONE %s %s..%s rows=%s", table, start, end, total)
    return total


def sync_summary(tw, bq, start, end, run_id):
    table = "summary_page_metrics"
    sink = Sink(bq, table, run_id, dated=True)
    total, d, w0 = 0, start, start
    while d <= end:
        # Date-only strings are read as midnight UTC, which returns the *previous*
        # shop day; send shop-local timestamps with the offset instead.
        a, b = day_bounds(d)
        r = tw.post("summary-page/get-data",
                    {"shopDomain": SHOP, "period": {"start": a, "end": b}, "todayHour": 25})
        for m in r.get("metrics") or []:
            # Drop the comparison-period fields; that day is stored on its own row anyway.
            m = {k: v for k, v in m.items() if k not in ("delta",)}
            if isinstance(m.get("values"), dict):
                m["values"] = {"current": m["values"].get("current")}
            if isinstance(m.get("charts"), dict):
                m["charts"] = {"current": m["charts"].get("current")}
            sink.add(d.isoformat(), m)
        if d == end or (d - w0).days >= 30:
            total += sink.commit(w0, d)
            w0 = d + dt.timedelta(days=1)
        d += dt.timedelta(days=1)
    log.info("DONE %s %s..%s rows=%s", table, start, end, total)
    return total


RUNS_SCHEMA = [
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("mode", "STRING"),
    bigquery.SchemaField("shop_id", "STRING"),
    bigquery.SchemaField("started_at", "TIMESTAMP"),
    bigquery.SchemaField("finished_at", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("sources", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("rows", "INT64"),
        bigquery.SchemaField("seconds", "FLOAT64"),
        bigquery.SchemaField("error", "STRING"),
    ]),
]


def record_run(bq, row):
    """Append one row to _sync_runs: the audit trail freshness checks read."""
    table = f"{PROJECT}.{DATASET}._sync_runs"
    t = bigquery.Table(table, schema=RUNS_SCHEMA)
    t.time_partitioning = bigquery.TimePartitioning(field="started_at")
    t.description = "One row per tw-sync run: status and rows per source."
    bq.create_table(t, exists_ok=True)
    cfg = bigquery.LoadJobConfig(schema=RUNS_SCHEMA, write_disposition="WRITE_APPEND")
    bq.load_table_from_json([row], table, job_config=cfg).result()


# --------------------------------------------------------------------------- main

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--backfill", action="store_true", help="pull full history")
    p.add_argument("--only", default="", help="comma-separated source names")
    p.add_argument("--start", help="override start date (YYYY-MM-DD)")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(threadName)s %(message)s")

    key = secretmanager.SecretManagerServiceClient().access_secret_version(
        name=f"projects/{PROJECT}/secrets/{SECRET}/versions/latest").payload.data.decode().strip()
    tw, bq = TripleWhale(key), bigquery.Client(project=PROJECT)
    run_id = uuid.uuid4().hex
    today = dt.datetime.now(SHOP_TZ).date()
    only = set(filter(None, args.only.split(",")))
    want = lambda name: not only or name in only
    daily_start = dt.date.fromisoformat(args.start) if args.start else today - dt.timedelta(days=LOOKBACK_DAYS)
    log.info("run %s mode=%s shop=%s", run_id, "backfill" if args.backfill else "daily", SHOP)

    failures, results = [], []
    started = dt.datetime.now(dt.timezone.utc)

    def guarded(name, fn, *a):
        t0 = time.monotonic()
        try:
            rows = fn(*a)
            results.append({"source": name, "status": "ok", "rows": rows,
                            "seconds": round(time.monotonic() - t0, 1), "error": None})
        except Exception as e:
            log.exception("FAILED %s", name)
            failures.append(f"{name}: {e}")
            results.append({"source": name, "status": "failed", "rows": None,
                            "seconds": round(time.monotonic() - t0, 1), "error": str(e)[:1000]})

    def sql_job(name):
        if name in DATED:
            cfg = DATED[name]
            start = (first_date(tw, name, cfg, daily_start) if args.backfill and not args.start
                     else daily_start)
            guarded(name, sync_dated, tw, bq, name, cfg, start, today, run_id)
        else:
            incremental = (not args.backfill and name in INCREMENTAL_SNAPSHOTS
                           and today.weekday() != FULL_REFRESH_WEEKDAY)
            guarded(name, sync_snapshot, tw, bq, name, run_id,
                    daily_start if incremental else None, today)

    def journeys_job():
        start = daily_start if not args.backfill or args.start else journeys_first_date(tw, today)
        guarded("orders_with_journeys", sync_journeys, tw, bq, start, today, run_id)

    def summary_job():
        # Summary Page history follows the ads/orders history.
        start = daily_start if not args.backfill or args.start else dt.date(2023, 8, 17)
        guarded("summary_page_metrics", sync_summary, tw, bq, start, today, run_id)

    sql_names = [n for n in list(DATED) + SNAPSHOT if want(n)]
    with ThreadPoolExecutor(SQL_WORKERS + 2, thread_name_prefix="w") as ex:
        futs = []
        if want("orders_with_journeys"):
            futs.append(ex.submit(journeys_job))
        if want("summary_page_metrics"):
            futs.append(ex.submit(summary_job))
        # SQL sources share one rate limit; SQL_WORKERS in flight keeps it busy.
        with ThreadPoolExecutor(SQL_WORKERS, thread_name_prefix="sql") as sx:
            list(sx.map(sql_job, sql_names))
        for f in futs:
            f.result()

    try:
        record_run(bq, {
            "run_id": run_id, "mode": "backfill" if args.backfill else "daily", "shop_id": SHOP,
            "started_at": started.isoformat(),
            "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "status": "failed" if failures else "ok", "sources": results})
    except Exception:
        log.exception("could not write _sync_runs")

    if failures:
        log.error("%d source(s) failed:\n  %s", len(failures), "\n  ".join(failures))
        sys.exit(1)
    log.info("run %s finished OK", run_id)


if __name__ == "__main__":
    main()
