# BGZ Triple Whale → BigQuery

Pulls BodyGuardz (`xjfdku-0t.myshopify.com`) data from the Triple Whale API into
BigQuery project `triplewhale-508216` every day at 06:00 America/Chicago.

## Layers

| Dataset | What | Built by |
|---|---|---|
| `triple_whale_raw` | One table per Triple Whale source. Every row: `shop_id, event_date, data JSON, _ingested_at, _run_id`. Plus `_sync_runs` (one row per run). | `main.py` (Cloud Run job `tw-sync`) |
| `triple_whale_staging` | One typed view per raw table (every JSON key as a column), plus `order_touchpoints`, `order_attribution`, `summary_page_metric_values`, `sync_runs`. | `build_views.py` |
| `triple_whale_curated` | Business views: `daily_kpis`, `marketing_performance_daily`, `channel_performance_daily`, `ad_spend_hourly`, `orders`, `product_performance_daily`, `email_sms_performance_daily`, `customers`, `pipeline_status`. | `build_curated.py` |

Staging and curated are *authorized datasets* on the layer below, so people can be
given read access to curated without access to raw.

`daily_kpis` comes from Triple Whale's Summary Page, so net profit, ROAS, MER,
NCPA etc. match the Triple Whale dashboard exactly.

## Sources

* SQL endpoint (`/orcabase/api/sql`): every documented table, `SELECT *`. Dated
  tables are replaced window by window (DELETE + INSERT in one transaction);
  tables without `event_date` are full snapshots every run.
* `/attribution/get-orders-with-journeys-v2` → `orders_with_journeys` (every order,
  all attribution models, full journey).
* `/summary-page/get-data` → `summary_page_metrics` (the whole Summary Page per day).

Not pulled: `customer_journey_table` (every pageview, 450M+ rows; order journeys
come from the attribution endpoint), `customer_segmentation_analytics_tvf`
(needs per-segment parameters).

Contact PII (names, phones, addresses) is dropped before loading — see
`PII_COLUMNS` in `main.py`. Triple Whale already masks emails.

Triple Whale keeps ~13 months of pixel, session and web-analytics data. The daily
run only re-pulls the last 7 days, so BigQuery keeps history beyond that.

## Operating

```bash
./deploy.sh v4                                   # build + deploy the job
gcloud run jobs execute tw-sync --region=us-central1                        # daily-style run now
gcloud run jobs execute tw-sync --region=us-central1 --args=--backfill       # full history
gcloud run jobs execute tw-sync --region=us-central1 --args="^@^--only@orders_table@--start@2026-01-01"
.venv/bin/python build_views.py && .venv/bin/python build_curated.py   # after schema changes
./setup_alerts.sh you@example.com                # needs monitoring roles
```

Health: `SELECT * FROM triple_whale_curated.pipeline_status ORDER BY days_behind DESC`.

## Identities

* `tw-pipeline@` runs the job: BigQuery `dataEditor` on `triple_whale_raw` only,
  `jobUser` on the project, `secretAccessor` on `triple-whale-api-key` only.
* `tw-scheduler@` is Cloud Scheduler's identity: project-level `run.invoker`.
  `tw-sync` is the only Cloud Run resource in the project. (An IAM condition on
  the job name was tried and made Scheduler get 403; a job-level binding needs
  `run.jobs.setIamPolicy`, which a project admin can do instead.)

## Triple Whale API gotchas

* Real limits: SQL 5/s and 100/min; journeys 10/s and 60/min. Responses max
  10 MiB / 50k rows. The job splits by date, then by hash bucket, on "too big".
* `cityHash64(*)` returns NULL if any column is NULL — bucket on key columns.
* `count()` on some views (e.g. `ads_table`, hourly) is lower than the rows
  `SELECT *` returns, and even `count()` over a `SELECT *` subquery can differ
  (the optimizer prunes columns, which changes how views aggregate). The raw
  tables hold exactly what `SELECT *` returns.
* `email_sms_table` has rows that repeat exactly; that is Triple Whale's grain
  (summing `recipients` matches Triple Whale's own `sum()`). `*_unique` metrics
  (opens, clicks) are de-duplicated by Triple Whale and must not be summed.
* Refund amounts are negative for Shopify/Amazon and positive for TikTok Shop;
  `curated.orders` normalizes them to positive.
* The SQL endpoint has sent `Retry-After: ~93000`; waits are capped at 120s.
* The Summary Page endpoint reads date-only `start`/`end` as midnight UTC and
  returns the *previous* shop day; the job sends shop-local timestamps.
* The API key belongs to a person's Triple Whale login; if their access goes,
  the sync stops. Rotate with `gcloud secrets versions add triple-whale-api-key`.
