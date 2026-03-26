SET SQLBLANKLINES ON
SET DEFINE OFF
-- OLAP Views
-- Notes:
--   Some consolidation views used upstream are limited by the access layer
--   to 10000 rows (especially activity/orders/products coming from REST-based
--   access views). As a result, OLAP outputs built on those sources reflect
--   the current sampled data window rather than full source-system volume.



-- ============================================================
-- ADINA
--
-- Required consolidation views:
--   V_CONS_USERS
--   V_CONS_SUBSCRIPTIONS
--   V_CONS_SUBSCRIPTION_TIERS
--   V_CONS_USER_SUBSCRIPTION
--   V_CONS_SUB_COHORT
--   V_CONS_USER_ACTIVITY
--   V_CONS_PG_ORDERS
--   V_CONS_USER_ORDERS
--
-- Required dimension views:
--   V_DIM_USERS
--   V_DIM_SUBSCRIPTION_TIERS

-- 01. V_OLAP_COHORT_RETENTION
--
-- Query 1:
--   Cohort retention / churn by subscription tier
--
-- Description:
--   Aggregates subscription lifecycle and retention data by cohort month
--   and subscription tier in order to measure user retention and churn.
--   The view exposes how many users started in each cohort, how many
--   were retained after 30 and 90 days, how many churned, and the
--   corresponding percentage metrics.
--
--   This OLAP view uses:
--   - V_CONS_SUB_COHORT as the fact-like source
--   - V_DIM_SUBSCRIPTION_TIERS as the descriptive dimension source
--
-- Technique:
--   GROUP BY ROLLUP
--   - detailed rows by cohort month and subscription tier
--   - subtotals by cohort month
--   - grand total

CREATE OR REPLACE VIEW FDBO.V_OLAP_COHORT_RETENTION AS
SELECT
    c.cohort_month,
    dt.tier_name,

    COUNT(*) AS users_started,
    SUM(c.retained_30d) AS retained_30d_users,
    SUM(c.retained_90d) AS retained_90d_users,
    SUM(c.churned_flag) AS churned_users,

    ROUND(
        100 * SUM(c.retained_30d) / NULLIF(COUNT(*), 0),
        2
    ) AS retention_30d_pct,

    ROUND(
        100 * SUM(c.retained_90d) / NULLIF(COUNT(*), 0),
        2
    ) AS retention_90d_pct,

    ROUND(
        100 * SUM(c.churned_flag) / NULLIF(COUNT(*), 0),
        2
    ) AS churn_pct,

    GROUPING(c.cohort_month) AS grp_cohort,
    GROUPING(dt.tier_name)   AS grp_tier

FROM FDBO.V_CONS_SUB_COHORT c
LEFT JOIN FDBO.V_DIM_SUBSCRIPTION_TIERS dt
    ON dt.tier_id = c.tier_id
GROUP BY ROLLUP (c.cohort_month, dt.tier_name);
/


-- 02. V_OLAP_ENGAGEMENT_REVENUE_MONTHLY
--
-- Query 2:
--   User engagement and order revenue analysis by month and country
--
-- Description:
--   Aggregates monthly behavioural activity and commercial order data
--   in order to analyse engagement intensity, purchase-related activity,
--   order volume, quantity sold, and revenue by month and user country.
--
--   The view combines:
--   - behavioural activity from V_CONS_USER_ACTIVITY
--   - order transactions from V_CONS_USER_ORDERS
--   - descriptive user geography from V_DIM_USERS
--
-- Technique:
--   GROUP BY ROLLUP
--   - detailed rows by month and country
--   - subtotals by month
--   - grand total
--
--   WINDOW FUNCTIONS
--   - cumulative revenue over time
--   - cumulative orders over time
--   - previous-month revenue comparison

CREATE OR REPLACE VIEW FDBO.V_OLAP_ENGAGEMENT_REVENUE_MONTHLY AS
WITH activity_monthly AS (
    SELECT
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM') AS activity_month,
        du.user_country_code,
        COUNT(*) AS total_events,
        SUM(CASE WHEN ua.event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,
        SUM(CASE WHEN ua.event_type = 'product_view' THEN 1 ELSE 0 END) AS total_product_views,
        SUM(CASE WHEN ua.event_type = 'search' THEN 1 ELSE 0 END) AS total_searches,
        SUM(CASE WHEN ua.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_add_to_cart,
        SUM(CASE WHEN ua.event_type = 'checkout_start' THEN 1 ELSE 0 END) AS total_checkout_starts,
        SUM(CASE WHEN ua.event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchase_events
    FROM FDBO.V_CONS_USER_ACTIVITY ua
    LEFT JOIN FDBO.V_DIM_USERS du
        ON du.user_id = ua.user_id
    GROUP BY
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM'),
        du.user_country_code
),
orders_monthly AS (
    SELECT
        TRUNC(CAST(uo.order_created_at AS DATE), 'MM') AS order_month,
        du.user_country_code,
        COUNT(DISTINCT uo.order_id) AS total_orders,
        SUM(NVL(uo.quantity, 0)) AS total_quantity,
        SUM(NVL(uo.line_total_usd, 0)) AS total_revenue_usd
    FROM FDBO.V_CONS_USER_ORDERS uo
    LEFT JOIN FDBO.V_DIM_USERS du
        ON du.user_id = uo.user_id
    GROUP BY
        TRUNC(CAST(uo.order_created_at AS DATE), 'MM'),
        du.user_country_code
),
combined AS (
    SELECT
        a.activity_month,
        a.user_country_code,
        a.total_events,
        a.total_page_views,
        a.total_product_views,
        a.total_searches,
        a.total_add_to_cart,
        a.total_checkout_starts,
        a.total_purchase_events,
        NVL(o.total_orders, 0) AS total_orders,
        NVL(o.total_quantity, 0) AS total_quantity,
        NVL(o.total_revenue_usd, 0) AS total_revenue_usd
    FROM activity_monthly a
    LEFT JOIN orders_monthly o
        ON o.order_month = a.activity_month
       AND NVL(o.user_country_code, '##NULL##') = NVL(a.user_country_code, '##NULL##')
),
base_rollup AS (
    SELECT
        activity_month,
        user_country_code,
        SUM(total_events) AS total_events,
        SUM(total_page_views) AS total_page_views,
        SUM(total_product_views) AS total_product_views,
        SUM(total_searches) AS total_searches,
        SUM(total_add_to_cart) AS total_add_to_cart,
        SUM(total_checkout_starts) AS total_checkout_starts,
        SUM(total_purchase_events) AS total_purchase_events,
        SUM(total_orders) AS total_orders,
        SUM(total_quantity) AS total_quantity,
        SUM(total_revenue_usd) AS total_revenue_usd,
        GROUPING(activity_month) AS grp_month,
        GROUPING(user_country_code) AS grp_country
    FROM combined
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
    m.prev_month_revenue_usd,
    b.grp_month,
    b.grp_country
FROM base_rollup b
LEFT JOIN monthly_totals m
    ON m.activity_month = b.activity_month;
/
--================================================================