-- ============================================================
-- OLAP VIEWS
--
-- Purpose:
--   This layer provides analytical views for reporting and business insights.
--
-- Description:
--   OLAP views aggregate and analyze data from fact views and dimension views.
--   They implement advanced analytical techniques such as:
--
--   - GROUP BY ROLLUP / CUBE (multi-dimensional aggregation)
--   - window functions (cumulative metrics, period comparison)
--   - derived KPIs (percentages, ratios, retention metrics)
--
--   These views are designed to answer business questions such as:
--   - retention and churn analysis
--   - engagement vs revenue analysis
--   - time-based performance trends
--
--   OLAP views represent the final analytical layer of the model.
--
-- Notes:
--   - Built on top of fact-like consolidation views (ROLAP approach).
--   - Use dimension views for descriptive context (e.g. user, tier).
--   - Results may reflect limited data volume due to upstream access layer constraints.
-- ============================================================
SET SQLBLANKLINES ON
SET DEFINE OFF

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
CREATE OR REPLACE VIEW FDBO.V_CONS_SUB_COHORT AS
SELECT
    us.user_id,
    us.email                AS user_email,
    us.full_name            AS user_full_name,
    us.country_code         AS user_country_code,
    us.city                 AS user_city,
    us.sub_id               AS subscription_id,
    us.tier_id,
    us.tier_name,
    us.sub_status           AS subscription_status,
    us.billing_cycle,
    us.started_at,
    us.current_period_start,
    us.current_period_end,
    us.cancelled_at,
    us.cancel_reason,
    us.monthly_price_usd,
    TRUNC(CAST(us.started_at AS DATE), 'MM')        AS cohort_month,
    CASE
        WHEN us.cancelled_at IS NOT NULL
            THEN CAST(us.cancelled_at AS DATE) - CAST(us.started_at AS DATE)
        WHEN us.current_period_end IS NOT NULL
            THEN CAST(us.current_period_end AS DATE) - CAST(us.started_at AS DATE)
        ELSE SYSDATE - CAST(us.started_at AS DATE)
    END AS days_active,
    CASE WHEN NVL(CAST(us.cancelled_at AS DATE), CAST(us.current_period_end AS DATE))
              >= CAST(us.started_at AS DATE) + 30 THEN 1 ELSE 0 END AS retained_30d,
    CASE WHEN NVL(CAST(us.cancelled_at AS DATE), CAST(us.current_period_end AS DATE))
              >= CAST(us.started_at AS DATE) + 90 THEN 1 ELSE 0 END AS retained_90d,
    CASE WHEN us.cancelled_at IS NOT NULL THEN 1 ELSE 0 END AS churned_flag
FROM FDBO.V_CONS_USER_SUBSCRIPTION us;
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
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM')   AS activity_month,
        ua.country_code,                        
        COUNT(*)                                     AS total_events,
        SUM(CASE WHEN ua.event_type = 'page_view'       THEN 1 ELSE 0 END) AS total_page_views,
        SUM(CASE WHEN ua.event_type = 'product_view'    THEN 1 ELSE 0 END) AS total_product_views,
        SUM(CASE WHEN ua.event_type = 'search'          THEN 1 ELSE 0 END) AS total_searches,
        SUM(CASE WHEN ua.event_type = 'add_to_cart'     THEN 1 ELSE 0 END) AS total_add_to_cart,
        SUM(CASE WHEN ua.event_type = 'checkout_start'  THEN 1 ELSE 0 END) AS total_checkout_starts,
        SUM(CASE WHEN ua.event_type = 'purchase'        THEN 1 ELSE 0 END) AS total_purchase_events
    FROM FDBO.V_CONS_USER_ACTIVITY ua
    GROUP BY
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM'),
        ua.country_code
),
orders_monthly AS (
    SELECT
        TRUNC(TO_DATE(SUBSTR(uo.order_created_at, 1, 10), 'YYYY-MM-DD'), 'MM') AS order_month, 
        uo.country_code                              AS user_country_code, 
        COUNT(DISTINCT uo.order_id)                  AS total_orders,
        SUM(NVL(uo.quantity, 0))                     AS total_quantity,
        SUM(NVL(uo.line_total_usd, 0))               AS total_revenue_usd
    FROM FDBO.V_CONS_USER_ORDERS uo
    GROUP BY
        TRUNC(TO_DATE(SUBSTR(uo.order_created_at, 1, 10), 'YYYY-MM-DD'), 'MM'),
        uo.country_code
),
combined AS (
    SELECT
        a.activity_month,
        a.country_code,
        a.total_events,
        a.total_page_views,
        a.total_product_views,
        a.total_searches,
        a.total_add_to_cart,
        a.total_checkout_starts,
        a.total_purchase_events,
        NVL(o.total_orders, 0)      AS total_orders,
        NVL(o.total_quantity, 0)    AS total_quantity,
        NVL(o.total_revenue_usd, 0) AS total_revenue_usd
    FROM activity_monthly a
    LEFT JOIN orders_monthly o
        ON  o.order_month = a.activity_month
        AND NVL(o.user_country_code, '##NULL##') = NVL(a.country_code, '##NULL##')
),
base_rollup AS (
    SELECT
        activity_month,
        country_code,
        SUM(total_events)           AS total_events,
        SUM(total_page_views)       AS total_page_views,
        SUM(total_product_views)    AS total_product_views,
        SUM(total_searches)         AS total_searches,
        SUM(total_add_to_cart)      AS total_add_to_cart,
        SUM(total_checkout_starts)  AS total_checkout_starts,
        SUM(total_purchase_events)  AS total_purchase_events,
        SUM(total_orders)           AS total_orders,
        SUM(total_quantity)         AS total_quantity,
        SUM(total_revenue_usd)      AS total_revenue_usd,
        GROUPING(activity_month)    AS grp_month,
        GROUPING(country_code) AS grp_country
    FROM combined
    GROUP BY ROLLUP(activity_month, country_code)
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
      AND country_code IS NULL
)
SELECT
    b.activity_month,
    b.country_code,
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
--================================================================
--================================================================
-- ============================================================
-- ============================================================
-- 3 : Geographic Market Density & Buyer-Seller Imbalance
--      ROLLUP(activity_band, country_code) + RANK + NTILE
--
--   geo_banded    →  activity band derivation using PERCENTILE_CONT OVER()
--   rollup_agg    →  ROLLUP only — no window functions
--   detail_ranked →  NTILE / RANK on detail rows only (is_subtotal = 0)
--   Final SELECT  →  RATIO_TO_REPORT + reconstruct subtotal rows via UNION ALL
-- ============================================================
WITH geo_banded AS (
    SELECT
        country_code,
        total_users,
        total_sellers,
        total_orders,
        CASE
            WHEN total_orders >= PERCENTILE_CONT(0.66)
                     WITHIN GROUP (ORDER BY total_orders) OVER () THEN 'High'
            WHEN total_orders >= PERCENTILE_CONT(0.33)
                     WITHIN GROUP (ORDER BY total_orders) OVER () THEN 'Mid'
            ELSE 'Low'
        END AS activity_band
    FROM FDBO.V_DIM_GEOGRAPHY
    WHERE country_code IS NOT NULL
),
rollup_agg AS (
    SELECT
        CASE GROUPING(activity_band) WHEN 1 THEN '** ALL BANDS **' ELSE activity_band END AS activity_band,
        CASE GROUPING(country_code)  WHEN 1 THEN NULL               ELSE country_code  END AS country_code,
        GROUPING(country_code)                                                             AS is_subtotal,
        SUM(total_users)                                                                   AS total_users,
        SUM(total_sellers)                                                                 AS total_sellers,
        SUM(total_orders)                                                                  AS total_orders,
        ROUND(SUM(total_users) / NULLIF(SUM(total_sellers), 0), 2)                        AS buyer_seller_ratio,
        CASE
            WHEN GROUPING(activity_band) = 1 AND GROUPING(country_code) = 1 THEN 'Grand Total'
            WHEN GROUPING(country_code)  = 1                                 THEN 'Activity Band Subtotal'
            ELSE 'Country Detail'
        END AS grouping_label
    FROM geo_banded
    GROUP BY ROLLUP(activity_band, country_code)
),
detail_ranked AS (
    SELECT
        activity_band,
        country_code,
        is_subtotal,
        total_users,
        total_sellers,
        total_orders,
        buyer_seller_ratio,
        grouping_label,
        NTILE(4) OVER (ORDER BY total_orders DESC) AS order_volume_quartile,
        RANK()   OVER (ORDER BY total_orders DESC) AS country_rank
    FROM rollup_agg
    WHERE is_subtotal = 0
)
SELECT
    activity_band,
    country_code,
    total_users,
    total_sellers,
    total_orders,
    buyer_seller_ratio,
    ROUND(RATIO_TO_REPORT(total_users) OVER () * 100, 2) AS user_share_pct,
    order_volume_quartile,
    country_rank,
    grouping_label
FROM detail_ranked
UNION ALL
SELECT
    activity_band,
    country_code,
    total_users,
    total_sellers,
    total_orders,
    buyer_seller_ratio,
    NULL AS user_share_pct,
    NULL AS order_volume_quartile,
    NULL AS country_rank,
    grouping_label
FROM rollup_agg
WHERE is_subtotal = 1
ORDER BY
    grouping_label DESC, 
    total_orders DESC NULLS LAST;
-- ============================================================
-- ============================================================
-- ============================================================
-- 4 : Product Affinity Revenue Impact by Type & Seller Pairing
--       (GROUPING SETS + RANK + SUM OVER)
--
-- Purpose:
--   Analyses product co-purchase pairs to understand which
--   affinity combinations (same/cross type, same/cross seller)
--   drive the highest co-purchase volume, and how individual
--   product pairs rank within their affinity category.

-- OLAP features:
--   GROUPING SETS((...detail...), (type), (seller))
--   RANK()   OVER (PARTITION BY affinity_type_category   ORDER BY co_purchase_count DESC)
--   SUM()    OVER (PARTITION BY affinity_seller_category ORDER BY co_purchase_count DESC
--                  ROWS UNBOUNDED PRECEDING)  — running co-purchase total per seller pairing
--   AVG()    OVER ()                          — global average for deviation flagging
-- Source view: V_DIM_PRODUCT_AFFINITY_TYPE
-- Output columns:
--   affinity_type_category    - same_type / cross_type  (NULL on seller subtotal rows)
--   affinity_seller_category  - same_seller / cross_seller (NULL on type subtotal rows)
--   product_1_id / product_2_id - pair identifiers (NULL on subtotal rows)
--   product_1_type / product_2_type
--   pair_count                - number of distinct pairs in group
--   total_co_purchases        - sum of co_purchase_count in group
--   avg_co_purchases          - average per pair
--   rank_in_type_category     - pair rank within affinity_type_category
--   running_co_purchases      - cumulative co-purchase count (per seller category)
--   vs_global_avg_flag        - 'above' / 'below' / 'subtotal' vs platform average
--   grouping_label
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_RPT_PRODUCT_AFFINITY_ANALYTICS AS
WITH affinity_base AS (
    SELECT /*+ NO_MERGE */
        affinity_type_category,
        affinity_seller_category,
        product_1_id,
        product_2_id,
        product_1_type,
        product_2_type,
        co_purchase_count
    FROM FDBO.V_DIM_PRODUCT_AFFINITY_TYPE
    WHERE product_1_id IS NOT NULL
      AND product_2_id IS NOT NULL
),
global_avg AS (
    SELECT AVG(co_purchase_count) AS avg_co_purchases_global
    FROM affinity_base
),
grouped_set AS (
    SELECT
        a.affinity_type_category,
        a.affinity_seller_category,
        a.product_1_id,
        a.product_2_id,
        a.product_1_type,
        a.product_2_type,
        g.avg_co_purchases_global,
        COUNT(*)                                     AS pair_count,
        SUM(a.co_purchase_count)                     AS total_co_purchases,
        AVG(a.co_purchase_count)                     AS avg_co_purchases_raw,
        GROUPING(a.product_1_id)                     AS is_subtotal,
        GROUPING(a.affinity_seller_category)         AS is_seller_sub,
        GROUPING(a.affinity_type_category)           AS is_type_sub
    FROM affinity_base a
    CROSS JOIN global_avg g
    GROUP BY GROUPING SETS (
        (a.affinity_type_category, a.affinity_seller_category, a.product_1_id, a.product_2_id, a.product_1_type, a.product_2_type, g.avg_co_purchases_global),
        (a.affinity_type_category, g.avg_co_purchases_global),
        (a.affinity_seller_category, g.avg_co_purchases_global)
    )
)
SELECT
    CASE is_type_sub   WHEN 1 THEN '** ALL **' ELSE affinity_type_category   END AS affinity_type_category,
    CASE is_seller_sub WHEN 1 THEN '** ALL **' ELSE affinity_seller_category END AS affinity_seller_category,
    CASE is_subtotal   WHEN 1 THEN NULL         ELSE product_1_id             END AS product_1_id,
    CASE is_subtotal   WHEN 1 THEN NULL         ELSE product_2_id             END AS product_2_id,
    pair_count,
    total_co_purchases,
    ROUND(avg_co_purchases_raw, 2)                                                AS avg_co_purchases,
    CASE is_subtotal
        WHEN 0 THEN RANK() OVER (PARTITION BY affinity_type_category, is_subtotal ORDER BY total_co_purchases DESC)
        ELSE NULL
    END                                                                           AS rank_in_type_category,
    CASE is_subtotal
        WHEN 0 THEN SUM(total_co_purchases) OVER (
                        PARTITION BY affinity_seller_category, is_subtotal 
                        ORDER BY total_co_purchases DESC 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ELSE NULL
    END                                                                           AS running_co_purchases,
    CASE is_subtotal
        WHEN 1 THEN 'subtotal'
        ELSE CASE WHEN avg_co_purchases_raw >= avg_co_purchases_global THEN 'above_avg' ELSE 'below_avg' END
    END                                                                           AS vs_global_avg_flag,
    CASE
        WHEN is_subtotal = 0   THEN 'Pair Detail'
        WHEN is_seller_sub = 1 THEN 'Type Category Subtotal'
        WHEN is_type_sub = 1   THEN 'Seller Category Subtotal'
        ELSE 'Other'
    END                                                                           AS grouping_label
FROM grouped_set;
-- ============================================================
-- ============================================================
-- ============================================================
-- 5. V_OLAP_SUB_RETENTION_COHORT
--
-- Purpose:
--   Performs longitudinal cohort analysis to track subscriber 
--   retention and cumulative attrition. It identifies how 
--   specific "joining groups" survive over time across tiers.
--
-- OLAP features: 
--   - Cumulative Windowing: SUM(...) OVER (Rows Unbounded Preceding) 
--     to track total loss since cohort inception.
--   - Moving Averages: AVG(...) OVER (Rows 2 Preceding) 
--     to smooth out seasonal churn spikes.
--   - Lead Analysis: LEAD(...) to project churn into the following month.
--
-- Source views: V_CONS_SUB_COHORT
--
-- Output columns:
--   tier_name             - Subscription plan name
--   cohort_month          - Original join month of the user group
--   cumulative_churned    - Total users lost from the original cohort
--   survival_rate_pct     - Percentage of users remaining active
--   rolling_3m_churn_rate - Smoothed monthly churn trend
--   next_month_churn_proj - Projected churn rate based on subsequent data
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_OLAP_SUB_RETENTION_COHORT AS
WITH cohort_monthly AS (
    SELECT
        tier_name,
        cohort_month,
        COUNT(*)          AS cohort_size,
        SUM(churned_flag) AS churned_this_month
    FROM FDBO.V_CONS_SUB_COHORT
    GROUP BY
        tier_name,
        cohort_month
),
with_windows AS (
    SELECT
        tier_name,
        cohort_month,
        cohort_size,
        churned_this_month,
        SUM(churned_this_month) OVER (
            PARTITION BY tier_name
            ORDER BY cohort_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                             AS cumulative_churned,
        ROUND(churned_this_month / NULLIF(cohort_size, 0) * 100, 2)   AS churn_rate_pct,
        ROUND(
            AVG(churned_this_month / NULLIF(cohort_size, 0) * 100) OVER (
                PARTITION BY tier_name
                ORDER BY cohort_month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 2)                                                     AS rolling_3m_churn_rate,
        LEAD(
            ROUND(churned_this_month / NULLIF(cohort_size, 0) * 100, 2),
            1, NULL
        ) OVER (PARTITION BY tier_name ORDER BY cohort_month)         AS next_month_churn_proj
    FROM cohort_monthly
)
SELECT
    tier_name,
    cohort_month,
    cohort_size,
    churned_this_month,
    cumulative_churned,
    ROUND(
        (cohort_size - cumulative_churned) / NULLIF(cohort_size, 0) * 100,
        2)                                                            AS survival_rate_pct,
    churn_rate_pct,
    rolling_3m_churn_rate,
    next_month_churn_proj
FROM with_windows;
-- ============================================================
-- ============================================================
-- ============================================================
-- 6. V_OLAP_SUBSCRIPTION_BILLING_TREND
--
-- Purpose:
--   Advanced financial time-series analysis for subscription revenue.
--   Calculates growth metrics, cumulative totals, and year-over-year (YoY)
--   comparisons using window functions.
--
-- OLAP feature: Window Functions (LAG, LEAD, SUM OVER, FIRST_VALUE, LAST_VALUE)
-- Source views: SUBSCRIPTION_INVOICES, SUBSCRIPTIONS, SUBSCRIPTION_TIERS
--
-- Output columns:
--   billing_month            - The first day of the billing month
--   tier_name                - The subscription plan name
--   total_billed_usd         - Current month revenue
--   cumulative_revenue_usd   - Running total of revenue per tier
--   prev_month_revenue_usd   - Prior month's revenue (for MoM calculation)
--   mom_growth_pct           - Month-over-Month growth percentage
--   same_month_prior_year_usd- Revenue from 12 months ago (for YoY calculation)
--   last_month_revenue_usd   - The revenue from the most recent month in the dataset
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_OLAP_SUBSCRIPTION_BILLING_TREND AS
WITH monthly_billing AS (
    SELECT
        TRUNC(si.billing_period_start, 'MM')    AS billing_month,
        st.name                                 AS tier_name,
        s.billing_cycle,
        COUNT(DISTINCT si.id)                   AS invoice_count,
        SUM(si.total_usd)                       AS total_billed_usd,
        SUM(si.discount_usd)                    AS total_discount_usd,
        SUM(si.tax_usd)                         AS total_tax_usd,
        COUNT(DISTINCT si.user_id)              AS unique_users_billed
    FROM FDBO.SUBSCRIPTION_INVOICES si
    JOIN FDBO.SUBSCRIPTIONS s
        ON s.id = si.subscription_id
    JOIN FDBO.SUBSCRIPTION_TIERS st
        ON st.id = s.tier_id
    WHERE si.status = 'paid'
    GROUP BY
        TRUNC(si.billing_period_start, 'MM'),
        st.name,
        s.billing_cycle
)
SELECT
    billing_month,
    tier_name,
    billing_cycle,
    invoice_count,
    total_billed_usd,
    total_discount_usd,
    total_tax_usd,
    unique_users_billed,
    SUM(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS UNBOUNDED PRECEDING
    )                                           AS cumulative_revenue_usd,
    LAG(total_billed_usd, 1) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
    )                                           AS prev_month_revenue_usd,
    LAG(total_billed_usd, 12) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
    )                                           AS same_month_prior_year_usd,
    LEAD(total_billed_usd, 1) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
    )                                           AS next_month_revenue_usd,
    FIRST_VALUE(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS UNBOUNDED PRECEDING
    )                                           AS first_month_revenue_usd,
    LAST_VALUE(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    )                                           AS last_month_revenue_usd,
    ROUND(
        100 * (
            total_billed_usd -
            LAG(total_billed_usd, 1) OVER (
                PARTITION BY tier_name
                ORDER BY billing_month
            )
        ) / NULLIF(
            LAG(total_billed_usd, 1) OVER (
                PARTITION BY tier_name
                ORDER BY billing_month
            ), 0
        ), 2
    )                                           AS mom_growth_pct
FROM monthly_billing
ORDER BY tier_name, billing_month;



-- ============================================================
-- ============================================================
-- ============================================================
-- 7. V_OLAP_FUNNEL_CONVERSION_BY_TIER
--
-- Purpose:
--   Analyzes the customer journey funnel (Awareness -> Consideration -> Conversion)
--   segmented by subscription tier and product category.
--   Provides granular conversion rates and purchase rates using multidimensional 
--   grouping sets for flexible reporting.
--
-- OLAP feature: GROUPING SETS (Tier, Stage, Product, Month)
-- Source views: V_CONS_USER_ACTIVITY, V_CONS_USER_SUBSCRIPTION
--
-- Output columns:
--   tier_name          - User's current subscription level
--   funnel_stage       - Categorized intent (Awareness/Consideration/Conversion)
--   product_type       - The type of product interacted with
--   event_month        - The month the activity occurred
--   total_events       - Raw count of all activities in that group
--   unique_users       - Distinct user count (Reach)
--   purchase_rate_pct   - Percentage of total events that were purchases
--   conversion_rate_pct - Percentage of events that were "lower-funnel" actions
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_OLAP_FUNNEL_CONVERSION_BY_TIER AS
WITH funnel_base AS (
    SELECT
        ua.id,
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM') AS event_month,
        us.tier_name,
        ua.product_type,
        ua.event_type,
        CASE
            WHEN ua.event_type IN ('page_view','search')
                THEN 'awareness'
            WHEN ua.event_type IN ('product_view','add_to_cart')
                THEN 'consideration'
            WHEN ua.event_type IN ('checkout_start','purchase')
                THEN 'conversion'
            ELSE 'other'
        END                             AS funnel_stage,
        CASE
            WHEN ua.event_type IN (
                'add_to_cart','checkout_start','purchase'
            ) THEN 1 ELSE 0
        END                             AS is_conversion_event,
        CASE
            WHEN ua.event_type = 'purchase' THEN 1
            ELSE 0
        END                             AS is_purchase
    FROM FDBO.V_CONS_USER_ACTIVITY ua
    LEFT JOIN (
        SELECT user_id, tier_name
        FROM (
            SELECT user_id, tier_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY user_id
                       ORDER BY started_at DESC
                   ) AS rn
            FROM FDBO.V_CONS_USER_SUBSCRIPTION
        ) WHERE rn = 1
    ) us ON us.user_id = ua.id
)
SELECT
    tier_name,
    funnel_stage,
    product_type,
    event_month,
    COUNT(*)                            AS total_events,
    SUM(is_conversion_event)            AS conversion_events,
    SUM(is_purchase)                    AS purchase_events,
    COUNT(DISTINCT id)             AS unique_users,
    ROUND(
        100 * SUM(is_purchase)
        / NULLIF(COUNT(*), 0), 2
    )                                   AS purchase_rate_pct,
    ROUND(
        100 * SUM(is_conversion_event)
        / NULLIF(COUNT(*), 0), 2
    )                                   AS conversion_rate_pct,
    GROUPING(tier_name)                 AS grp_tier,
    GROUPING(funnel_stage)              AS grp_funnel,
    GROUPING(product_type)              AS grp_product_type,
    GROUPING(event_month)               AS grp_month
FROM funnel_base
GROUP BY GROUPING SETS (
    (tier_name, funnel_stage),
    (tier_name, funnel_stage, product_type),
    (tier_name, funnel_stage, event_month),
    ()
);
select * from V_OLAP_FUNNEL_CONVERSION_BY_TIER

-- ============================================================
-- ============================================================
-- ============================================================
-- 8. V_OLAP_INVOICE_PAYMENT_BEHAVIOUR
--
-- Purpose:
--   Analyzes subscription invoice payment behavior and financial metrics
--   segmented by subscription tier, billing cycle, and country.
--   Provides comprehensive payment performance, discount impact, and
--   payment speed correlations using multidimensional aggregation.
--
-- OLAP feature: CUBE (Tier, Billing Cycle, Country)
-- Source views: SUBSCRIPTION_INVOICES, SUBSCRIPTIONS, SUBSCRIPTION_TIERS, USERS
--
-- Output columns:
--   tier_name                       - Subscription plan name
--   billing_cycle                   - Billing frequency (monthly/annual)
--   country_code                    - User's country code
--   total_invoices                  - Count of invoices in group
--   paid_count                      - Number of paid invoices
--   overdue_count                   - Number of overdue invoices
--   discounted_count                - Number of invoices with discounts
--   payment_rate_pct                - Percentage of invoices paid
--   overdue_rate_pct                - Percentage of invoices overdue
--   avg_invoice_usd                 - Mean invoice total
--   median_invoice_usd              - Median invoice total
--   stddev_invoice_usd              - Standard deviation of invoice totals
--   p25_invoice_usd                 - 25th percentile invoice total
--   p75_invoice_usd                 - 75th percentile invoice total
--   avg_discount_rate_pct           - Mean discount percentage
--   median_days_to_pay              - Median days from due date to payment
--   stddev_days_to_pay              - Standard deviation of payment delays
--   discount_payment_correlation    - Correlation between discount rate and payment
--   invoice_size_payment_speed_corr - Correlation between invoice size and payment speed
--   grouping_label                  - Hierarchical grouping level indicator
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_OLAP_INVOICE_PAYMENT_BEHAVIOUR AS
WITH invoice_base AS (
    SELECT
        si.id                               AS invoice_id,
        si.user_id,
        st.name                             AS tier_name,
        s.billing_cycle,
        u.country_code,
        si.status                           AS invoice_status,
        si.total_usd,
        si.subtotal_usd,
        si.discount_usd,
        si.tax_usd,
        ROUND(
            si.discount_usd
            / NULLIF(si.subtotal_usd, 0) * 100, 2
        )                                   AS discount_rate_pct,
        CASE
            WHEN si.paid_at IS NOT NULL
            THEN CAST(si.paid_at AS DATE)
               - CAST(si.due_at AS DATE)
            ELSE NULL
        END                                 AS days_to_pay,
        CASE
            WHEN si.status = 'paid'    THEN 1 ELSE 0
        END                                 AS is_paid,
        CASE
            WHEN si.status = 'overdue' THEN 1 ELSE 0
        END                                 AS is_overdue,
        CASE
            WHEN si.discount_usd > 0   THEN 1 ELSE 0
        END                                 AS has_discount
    FROM FDBO.SUBSCRIPTION_INVOICES si
    JOIN FDBO.SUBSCRIPTIONS s
        ON s.id = si.subscription_id
    JOIN FDBO.SUBSCRIPTION_TIERS st
        ON st.id = s.tier_id
    JOIN FDBO.USERS u
        ON u.id = si.user_id
)
SELECT
    tier_name,
    billing_cycle,
    country_code,
    COUNT(invoice_id)                       AS total_invoices,
    SUM(is_paid)                            AS paid_count,
    SUM(is_overdue)                         AS overdue_count,
    SUM(has_discount)                       AS discounted_count,
    ROUND(
        100 * SUM(is_paid)
        / NULLIF(COUNT(invoice_id), 0), 2
    )                                       AS payment_rate_pct,
    ROUND(
        100 * SUM(is_overdue)
        / NULLIF(COUNT(invoice_id), 0), 2
    )                                       AS overdue_rate_pct,
    ROUND(AVG(total_usd), 2)                AS avg_invoice_usd,
    ROUND(MEDIAN(total_usd), 2)             AS median_invoice_usd,
    ROUND(STDDEV(total_usd), 2)             AS stddev_invoice_usd,
    ROUND(
        PERCENTILE_CONT(0.25) WITHIN GROUP (
            ORDER BY total_usd
        ), 2
    )                                       AS p25_invoice_usd,
    ROUND(
        PERCENTILE_CONT(0.75) WITHIN GROUP (
            ORDER BY total_usd
        ), 2
    )                                       AS p75_invoice_usd,
    ROUND(AVG(discount_rate_pct), 2)        AS avg_discount_rate_pct,
    ROUND(MEDIAN(days_to_pay), 1)           AS median_days_to_pay,
    ROUND(STDDEV(days_to_pay), 2)           AS stddev_days_to_pay,
    ROUND(
        CORR(discount_rate_pct, is_paid), 4
    )                                       AS discount_payment_correlation,
    ROUND(
        CORR(total_usd, days_to_pay), 4
    )                                       AS invoice_size_payment_speed_corr,
    CASE
        WHEN GROUPING(tier_name) = 1
         AND GROUPING(billing_cycle) = 1
         AND GROUPING(country_code) = 1
            THEN 'Grand Total'
        WHEN GROUPING(billing_cycle) = 1
         AND GROUPING(country_code) = 1
            THEN 'Tier Subtotal'
        WHEN GROUPING(country_code) = 1
            THEN 'Tier + Cycle Subtotal'
        ELSE 'Detail'
    END                                     AS grouping_label,
    GROUPING(tier_name)                     AS grp_tier,
    GROUPING(billing_cycle)                 AS grp_cycle,
    GROUPING(country_code)                  AS grp_country
FROM invoice_base
GROUP BY CUBE(
    tier_name,
    billing_cycle,
    country_code
);

SELECT * FROM V_OLAP_INVOICE_PAYMENT_BEHAVIOUR