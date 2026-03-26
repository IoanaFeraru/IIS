SET SQLBLANKLINES ON
SET DEFINE OFF
-- Script 03: OLAP Views
-- Notes:
--   Some consolidation views used upstream are limited by the access layer
--   to 10000 rows (especially activity/orders/products coming from REST-based
--   access views). As a result, OLAP outputs built on those sources reflect
--   the current sampled data window rather than full source-system volume.

-- ============================================================
-- ADINA
--
-- Required consolidation views:
--   V_CONS_USER_SUBSCRIPTION
--   V_CONS_SUB_COHORT
--   V_CONS_USER_ACTIVITY
--   V_CONS_USER_ORDERS
--   V_CONS_USER_ENGAGEMENT_SALES
-

-- 01. V_OLAP_COHORT_RETENTION
--
-- Query 1:
--   Cohort retention / churn by subscription tier
--
-- Description:
--   Aggregates subscription lifecycle data by cohort month and
--   subscription tier in order to measure user retention and churn.
--   The view exposes the number of users who started in each cohort,
--   how many were retained after 30 and 90 days, how many churned,
--   and the corresponding percentage metrics.
--
-- Technique:
--   GROUP BY ROLLUP
--   - multi-dimensional aggregation
--   - subtotals by cohort and tier
--   - grand total

CREATE OR REPLACE VIEW FDBO.V_OLAP_COHORT_RETENTION AS
SELECT
    cohort_month,
    tier_name,

    COUNT(*) AS users_started,
    SUM(retained_30d) AS retained_30d_users,
    SUM(retained_90d) AS retained_90d_users,
    SUM(churned_flag) AS churned_users,

    ROUND(
        100 * SUM(retained_30d) / NULLIF(COUNT(*), 0),
        2
    ) AS retention_30d_pct,

    ROUND(
        100 * SUM(retained_90d) / NULLIF(COUNT(*), 0),
        2
    ) AS retention_90d_pct,

    ROUND(
        100 * SUM(churned_flag) / NULLIF(COUNT(*), 0),
        2
    ) AS churn_pct,

    GROUPING(cohort_month) AS grp_cohort,
    GROUPING(tier_name)    AS grp_tier

FROM FDBO.V_CONS_SUB_COHORT
GROUP BY ROLLUP (cohort_month, tier_name);

-- 02. V_OLAP_ENGAGEMENT_REVENUE_MONTHLY
--
-- Query 2:
--   User engagement and order revenue analysis by month and country
--
-- Description:
--   Aggregates monthly behavioural activity from TimescaleDB events,
--   enriched with Oracle user geography and matched with PostgreSQL
--   order data. The view measures engagement intensity, purchase-related
--   activity, order volume, quantity sold, and revenue, while also
--   exposing cumulative metrics and previous-month revenue comparison.
--
-- Technique:
--   GROUP BY ROLLUP
--   - monthly and country-level subtotals
--   - grand total aggregation
--
--   WINDOW FUNCTIONS
--   - cumulative revenue over time
--   - cumulative orders over time
--   - previous-month revenue comparison
CREATE OR REPLACE VIEW FDBO.V_OLAP_ENGAGEMENT_REVENUE_MONTHLY AS
WITH base_rollup AS (
    SELECT
        activity_month,
        user_country_code,
        SUM(total_events) AS total_events,
        SUM(page_views) AS total_page_views,
        SUM(product_views) AS total_product_views,
        SUM(searches) AS total_searches,
        SUM(add_to_cart_events) AS total_add_to_cart,
        SUM(checkout_starts) AS total_checkout_starts,
        SUM(purchase_events) AS total_purchase_events,
        SUM(order_count) AS total_orders,
        SUM(total_quantity) AS total_quantity,
        SUM(total_revenue_usd) AS total_revenue_usd
    FROM FDBO.V_CONS_USER_ENGAGEMENT_SALES
    GROUP BY ROLLUP(activity_month, user_country_code)
),
monthly_totals AS (
    SELECT
        activity_month,
        total_revenue_usd,
        total_orders,
        SUM(total_revenue_usd) OVER (
            ORDER BY activity_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_revenue_usd,
        SUM(total_orders) OVER (
            ORDER BY activity_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_orders,
        LAG(total_revenue_usd) OVER (
            ORDER BY activity_month
        ) AS prev_month_revenue_usd
    FROM base_rollup
    WHERE activity_month IS NOT NULL
      AND user_country_code IS NULL
)
SELECT
    b.activity_month,
    b.user_country_code,
    b.total_events,
    b.total_page_views,
    b.total_product_views,
    b.total_searches,
    b.total_add_to_cart,
    b.total_checkout_starts,
    b.total_purchase_events,
    b.total_orders,
    b.total_quantity,
    b.total_revenue_usd,
    m.cumulative_revenue_usd,
    m.cumulative_orders,
    m.prev_month_revenue_usd
FROM base_rollup b
LEFT JOIN monthly_totals m
    ON m.activity_month = b.activity_month;
/

