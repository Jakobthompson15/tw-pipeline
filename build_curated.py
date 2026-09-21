"""Build triple_whale_curated: business-ready views on top of triple_whale_staging.

  python build_curated.py            # (re)create all curated views
  python build_curated.py --dry-run  # print the SQL only

Numbers that Triple Whale itself computes (net profit, ROAS, MER, NCPA, LTV...)
come straight from the Summary Page so they match the Triple Whale dashboard.
"""

import argparse

from google.cloud import bigquery

PROJECT = "triplewhale-508216"
STG = f"`{PROJECT}.triple_whale_staging`"
CUR = f"{PROJECT}.triple_whale_curated"

# Summary Page metric_id -> column name in daily_kpis.
KPIS = {
    # Revenue (Shopify)
    "totalSales": "order_revenue",
    "netSales": "total_sales",
    "topKpiGrossSales": "gross_sales",
    "totalDiscounts": "discounts",
    "totalRefunds": "returns",
    "totalReturns": "returns_pct",
    "totalTaxes": "taxes",
    "totalShippingPrice": "shipping_price",
    "blendedSales": "blended_sales_incl_amazon",
    # Orders & customers
    "totalOrders": "orders",
    "totalOrdersWithAmount": "orders_over_zero",
    "shopifyAovIncludeZero": "aov",
    "shopifyAov": "true_aov",
    "totalVariantsSold": "units_sold",
    "newCustomersOrders": "new_customer_orders",
    "newCustomerSales": "new_customer_revenue",
    "returningCustomerOrders": "returning_customer_orders",
    "rcRevenue": "returning_customer_revenue",
    "newCustomersPercent": "new_customers_pct",
    "getUniqueCustomerCount": "unique_customers",
    "uniqueCustomerLifetimeValue": "ltv",
    # Costs & profit
    "totalProductCosts": "cogs",
    "totalPaymentGatewayCosts": "payment_gateway_costs",
    "grossProfit": "gross_profit",
    "totalNetProfit": "net_profit",
    "totalNetMargin": "net_margin_pct",
    "cashTurnover": "cash_turnover",
    # Marketing efficiency
    "blendedAds": "blended_ad_spend",
    "totalRoas": "blended_roas",
    "mer": "mer_pct",
    "totalCpa": "blended_cpa",
    "newCustomersCpa": "ncpa",
    "newCustomersRoas": "new_customer_roas",
    "poas": "poas",
    "ltvCpa": "ltv_to_cpa",
    "blendedAttributedRoas": "blended_attributed_roas",
    # Spend by channel
    "fb_ads_spend": "facebook_spend",
    "ga_adCost": "google_spend",
    "tiktok_spend": "tiktok_spend",
    "bingAdSpend": "microsoft_spend",
    "pinterestSpend": "pinterest_spend",
    "openaiAdsSpend": "chatgpt_ads_spend",
    "amazonAds": "amazon_ads_spend",
    # Channel-reported ROAS
    "fb_ads_purchase_roas": "facebook_roas",
    "ga_ROAS": "google_roas",
    "tiktok_complete_payment_roas": "tiktok_roas",
    # Amazon
    "totalAmazonSales": "amazon_sales",
    "totalAmazonOrders": "amazon_orders",
    "amazonNetProfit": "amazon_net_profit",
    "amazonROAS": "amazon_roas",
    # Site (Triple Pixel)
    "pixelVisitSessions": "sessions",
    "pixelUniqueVisitors": "users",
    "pixelConversionRate": "conversion_rate_pct",
    "pixelPercentAtc": "add_to_cart_pct",
    "pixelBounceRate": "bounce_rate_pct",
}

VIEWS = {}

VIEWS["daily_kpis"] = f"""
-- One row per day with Triple Whale's own Summary Page KPIs.
-- The current day is partial until the next daily run re-pulls it.
SELECT
  event_date AS date,
{",\n".join(f"  MAX(IF(metric_id = '{m}', value, NULL)) AS {c}" for m, c in KPIS.items())}
FROM {STG}.summary_page_metric_values
GROUP BY date"""

VIEWS["marketing_performance_daily"] = f"""
-- Spend, platform-reported and Triple Whale pixel-attributed results per ad per day.
-- Attribution: Triple Attribution model, lifetime window (Triple Whale default).
SELECT
  event_date AS date,
  channel, channel_type, account_id,
  campaign_id, ANY_VALUE(campaign_name) AS campaign_name,
  adset_id, ANY_VALUE(adset_name) AS adset_name,
  ad_id, ANY_VALUE(ad_name) AS ad_name,
  ANY_VALUE(model) AS attribution_model,
  ANY_VALUE(attribution_window) AS attribution_window,
  SUM(spend) AS spend,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(outbound_clicks) AS outbound_clicks,
  SUM(channel_reported_conversions) AS platform_purchases,
  SUM(channel_reported_conversion_value) AS platform_revenue,
  SUM(website_purchases) AS pixel_purchases,
  SUM(order_revenue) AS pixel_revenue,
  SUM(new_customer_orders) AS pixel_new_customer_orders,
  SUM(new_customer_order_revenue) AS pixel_new_customer_revenue,
  SUM(visit_sessions) AS sessions,
  SUM(add_to_carts) AS add_to_carts,
  SUM(checkouts) AS checkouts,
  SAFE_DIVIDE(SUM(order_revenue), SUM(spend)) AS pixel_roas,
  SAFE_DIVIDE(SUM(channel_reported_conversion_value), SUM(spend)) AS platform_roas,
  SAFE_DIVIDE(SUM(spend), SUM(website_purchases)) AS pixel_cpa,
  SAFE_DIVIDE(SUM(spend), SUM(new_customer_orders)) AS pixel_ncpa,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
  SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
  SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm
FROM {STG}.pixel_joined
GROUP BY date, channel, channel_type, account_id, campaign_id, adset_id, ad_id"""

VIEWS["channel_performance_daily"] = f"""
-- marketing_performance_daily rolled up to channel.
SELECT
  date, channel,
  SUM(spend) AS spend, SUM(impressions) AS impressions, SUM(clicks) AS clicks,
  SUM(platform_purchases) AS platform_purchases, SUM(platform_revenue) AS platform_revenue,
  SUM(pixel_purchases) AS pixel_purchases, SUM(pixel_revenue) AS pixel_revenue,
  SUM(pixel_new_customer_orders) AS pixel_new_customer_orders,
  SAFE_DIVIDE(SUM(pixel_revenue), SUM(spend)) AS pixel_roas,
  SAFE_DIVIDE(SUM(platform_revenue), SUM(spend)) AS platform_roas,
  SAFE_DIVIDE(SUM(spend), SUM(pixel_new_customer_orders)) AS pixel_ncpa
FROM `{CUR}.marketing_performance_daily`
GROUP BY date, channel"""

VIEWS["ad_spend_hourly"] = f"""
-- Platform-reported ad delivery by hour (ads_table grain), for pacing analysis.
SELECT
  event_date AS date, event_hour AS hour, channel, account_id,
  campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name,
  spend, impressions, clicks, conversions AS platform_purchases,
  conversion_value AS platform_revenue
FROM {STG}.ads"""

VIEWS["orders"] = f"""
-- One row per order (all sales platforms) with refunds and pixel first/last click.
WITH refunds AS (
  SELECT order_id,
    -- Triple Whale signs refunds negative for Shopify/Amazon and positive for
    -- TikTok Shop; report them as positive amounts.
    SUM(ABS(total_refunded_price)) AS refunded_amount,
    SUM(ABS(total_refunded_tax)) AS refunded_tax,
    SUM(ABS(total_refunded_shipping)) AS refunded_shipping,
    COUNT(*) AS refund_count,
    MIN(processed_at) AS first_refund_at
  FROM {STG}.refunds
  GROUP BY order_id),
touch AS (
  SELECT order_id,
    MAX(IF(model = 'fullFirstClick', source, NULL)) AS first_click_source,
    MAX(IF(model = 'fullFirstClick', campaign_id, NULL)) AS first_click_campaign_id,
    MAX(IF(model = 'fullLastClick', source, NULL)) AS last_click_source,
    MAX(IF(model = 'fullLastClick', campaign_id, NULL)) AS last_click_campaign_id,
    MAX(IF(model = 'lastPlatformClick', source, NULL)) AS last_platform_click_source
  FROM {STG}.order_attribution
  WHERE click_number = 1 OR model IN ('fullLastClick', 'lastPlatformClick')
  GROUP BY order_id)
SELECT
  o.event_date AS order_date,
  o.order_id, o.order_name, o.platform, o.source_name,
  o.created_at, o.processed_at, o.cancelled_at,
  o.customer_id, o.is_new_customer, o.is_subscription_order,
  o.order_status, o.fulfillment_status, o.currency,
  o.gross_sales, o.discount_amount, o.discount_code, o.taxes, o.shipping_price,
  o.order_revenue, o.cost_of_goods, o.shipping_costs, o.payment_gateway_costs,
  o.handling_fees, o.product_quantity_sold_in_order AS units,
  o.customer_from_country_code AS country_code, o.customer_from_state_code AS state_code,
  IFNULL(r.refunded_amount, 0) AS refunded_amount,
  r.refund_count, r.first_refund_at,
  o.order_revenue - IFNULL(r.refunded_amount, 0) AS net_revenue_after_refunds,
  t.first_click_source, t.first_click_campaign_id,
  t.last_click_source, t.last_click_campaign_id, t.last_platform_click_source,
  o.products_info
FROM {STG}.orders o
LEFT JOIN refunds r USING (order_id)
LEFT JOIN touch t USING (order_id)"""

VIEWS["product_performance_daily"] = f"""
-- Product / variant sales, ads and site behaviour per day (Triple Whale Product Analytics).
SELECT
  event_date AS date, entity, product_id, variant_id, sku,
  product_title, variant_title, vendor, product_status,
  orders, total_items_sold AS units_sold, revenue, total_order_value,
  new_customer_orders, new_customer_revenue, returns,
  spend, impressions, clicks, visits, added_to_cart_events, added_to_cart_items,
  inventory_quantity,
  SAFE_DIVIDE(revenue, spend) AS roas
FROM {STG}.product_analytics"""

VIEWS["email_sms_performance_daily"] = f"""
SELECT * EXCEPT (_ingested_at) FROM {STG}.email_sms"""

VIEWS["customers"] = f"""
-- Customer snapshot (contact details removed at ingestion).
SELECT * EXCEPT (_ingested_at, event_date) FROM {STG}.customers"""

def pipeline_status_sql(bq):
    """Latest data date and load time per raw source, plus the last sync run's result."""
    raw = f"{PROJECT}.triple_whale_raw"
    tables = sorted(t.table_id for t in bq.list_tables(raw) if not t.table_id.startswith("_"))
    union = "\n  UNION ALL ".join(
        f"SELECT '{t}' AS source, MAX(event_date) AS latest_data_date, "
        f"MAX(_ingested_at) AS last_loaded_at, COUNT(*) AS total_rows FROM `{raw}.{t}`"
        for t in tables)
    return f"""
WITH sources AS (
  {union}),
last_run AS (
  SELECT * FROM {STG}.sync_runs
  QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1)
SELECT
  s.source, s.latest_data_date, s.last_loaded_at, s.total_rows,
  DATE_DIFF(CURRENT_DATE('America/Chicago'), s.latest_data_date, DAY) AS days_behind,
  x.status AS last_run_source_status, x.error AS last_run_source_error,
  r.run_id AS last_run_id, r.mode AS last_run_mode, r.status AS last_run_status,
  r.started_at AS last_run_started, r.finished_at AS last_run_finished
FROM sources s
LEFT JOIN last_run r ON TRUE
LEFT JOIN UNNEST(r.sources) x ON x.source = s.source"""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    only = set(filter(None, args.only.split(",")))
    bq = bigquery.Client(project=PROJECT)
    VIEWS["pipeline_status"] = pipeline_status_sql(bq)
    for name, sql in VIEWS.items():  # order matters: channel_* reads marketing_*
        if only and name not in only:
            continue
        ddl = f"CREATE OR REPLACE VIEW `{CUR}.{name}` AS\n{sql}"
        if args.dry_run:
            print(ddl, ";\n")
            continue
        bq.query(ddl).result()
        print("view", f"triple_whale_curated.{name}", flush=True)


if __name__ == "__main__":
    main()
