--------------------------------------------------------------------------------
--- IIS_SparkSQL_OLAP_Multidimensional_Analytical.sql
---
--- Multidimensional ROLAP Analytics over persisted offline tables:
---   pg_orders_persisted              (DS1 - PostgreSQL)
---   pg_order_items_persisted         (DS1 - PostgreSQL)
---   pg_marketplace_invoices_persisted(DS1 - PostgreSQL)
---   orcl_users_persisted             (DS2 - Oracle)
---   orcl_subscriptions_persisted     (DS2 - Oracle)
---   orcl_sub_invoices_persisted      (DS2 - Oracle)
---   orcl_sub_invoice_lines_persisted (DS2 - Oracle)
---   orcl_sub_tiers_persisted         (DS2 - Oracle)
---   orcl_tier_pricing_persisted      (DS2 - Oracle)
---   ts_events_persisted              (DS3 - TimescaleDB)
---   mg_products_persisted            (DS4 - MongoDB)
---   neo4j_bought_with_persisted      (DS5 - Neo4j)
---   csv_seller_profiles_persisted    (DS0 - CSV)
---
--- STRUCTURE:
---   LAYER 1 — Consolidation Views  (cross-source joins, normalisation)
---   LAYER 2 — Dimension Views      (descriptive attributes, hierarchies)
---   LAYER 3 — Fact Views           (measures, keys)
---   LAYER 4 — OLAP Analytical Views (ROLLUP, CUBE, WINDOW, PIVOT, UNPIVOT,
---                                    RANK, PERCENTILE, statistical functions)
---
--------------------------------------------------------------------------------
================================================================================
--- LAYER 1: CONSOLIDATION VIEWS
--- Purpose: cross-source joins, normalization, base for all upper layers
================================================================================

--------------------------------------------------------------------------------
--- C1. CONS_USER_SUBSCRIPTIONS
--- Oracle users + subscriptions + tiers + pricing
--- Sources: DS2 (Oracle offline)
--- Used by: D1, D3, F1, F3
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW cons_user_subscriptions AS
SELECT
    u.id                        AS user_id,
    u.email,
    u.fullName                  AS full_name,
    u.countryCode               AS country_code,
    u.city,
    u.createdAt                 AS user_created_at,
    u.isActive                  AS user_is_active,
    s.id                        AS sub_id,
    s.status                    AS sub_status,
    s.startedAt                 AS started_at,
    s.currentPeriodStart        AS current_period_start,
    s.currentPeriodEnd          AS current_period_end,
    s.cancelledAt               AS cancelled_at,
    s.billingCycle              AS billing_cycle,
    s.tierId                    AS tier_id,
    t.name                      AS tier_name,
    p.monthlyPriceUsd           AS monthly_price_usd
FROM orcl_users_persisted u
JOIN orcl_subscriptions_persisted s
    ON s.userId = u.id
LEFT JOIN orcl_sub_tiers_persisted t
    ON t.id = s.tierId
LEFT JOIN orcl_tier_pricing_persisted p
    ON p.tierId = s.tierId
   AND s.startedAt >= p.validFrom
   AND (p.validTo IS NULL OR s.startedAt < p.validTo);

SELECT * FROM cons_user_subscriptions LIMIT 10;
================================================================================
--- LAYER 2: DIMENSION VIEWS
--- Purpose: descriptive hierarchies for slicing and dicing fact data
================================================================================

--------------------------------------------------------------------------------
--- D1. DIM_USER_SUBSCRIPTION_TIER
--- User → Subscription → Tier hierarchy with cohort month and churn flags
--- Sources: cons_user_subscriptions
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_user_subscription_tier AS
SELECT
    user_id,
    full_name,
    country_code,
    city,
    user_is_active,
    sub_id,
    sub_status,
    tier_id,
    tier_name,
    billing_cycle,
    started_at,
    current_period_end,
    cancelled_at,
    monthly_price_usd,
    date_format(started_at, 'yyyy-MM')          AS cohort_month,
    CASE
        WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0
    END                                         AS churned_flag,
    CASE
        WHEN cancelled_at IS NOT NULL
            THEN datediff(CAST(cancelled_at AS DATE), CAST(started_at AS DATE))
        WHEN current_period_end IS NOT NULL
            THEN datediff(CAST(current_period_end AS DATE), CAST(started_at AS DATE))
        ELSE datediff(current_date(), CAST(started_at AS DATE))
    END                                         AS days_active,
    CASE
        WHEN cancelled_at IS NULL OR
             datediff(CAST(cancelled_at AS DATE), CAST(started_at AS DATE)) >= 30
        THEN 1 ELSE 0
    END                                         AS retained_30d,
    CASE
        WHEN cancelled_at IS NULL OR
             datediff(CAST(cancelled_at AS DATE), CAST(started_at AS DATE)) >= 90
        THEN 1 ELSE 0
    END                                         AS retained_90d
FROM cons_user_subscriptions;

SELECT * FROM dim_user_subscription_tier LIMIT 10;

--------------------------------------------------------------------------------
--- D2. DIM_TIME_CALENDAR
--- Time hierarchy derived from order and event dates
--- Sources: cons_order_transactions, cons_user_activity
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_time_calendar AS
SELECT DISTINCT
    dt                                          AS date_key,
    EXTRACT(year  FROM CAST(dt AS DATE))        AS year_no,
    EXTRACT(month FROM CAST(dt AS DATE))        AS month_no,
    EXTRACT(quarter FROM CAST(dt AS DATE))      AS quarter_no,
    date_format(CAST(dt AS DATE), 'yyyy-MM')    AS year_month
FROM (
    SELECT order_created_at AS dt FROM cons_order_transactions
        WHERE order_created_at IS NOT NULL
    UNION
    SELECT occurred_at AS dt FROM cons_user_activity
        WHERE occurred_at IS NOT NULL
);

SELECT * FROM dim_time_calendar LIMIT 10;

--------------------------------------------------------------------------------
--- D3. DIM_PRODUCT_PRICE_BAND
--- MongoDB products with price segmentation and seller info
--- Sources: mg_products_persisted, csv_seller_profiles_persisted
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_product_price_band AS
SELECT
    p.id                        AS product_id,
    p.name                      AS product_name,
    p.productType               AS product_type,
    p.priceUsd                  AS price_usd,
    p.isActive                  AS is_active,
    p.sellerId                  AS seller_id,
    s.displayName               AS seller_name,
    s.countryCode               AS seller_country,
    s.isVerified                AS seller_is_verified,
    CASE
        WHEN p.priceUsd < 20   THEN 'budget'
        WHEN p.priceUsd < 60   THEN 'mid_range'
        WHEN p.priceUsd < 120  THEN 'premium'
        ELSE 'luxury'
    END                         AS price_band
FROM mg_products_persisted p
LEFT JOIN csv_seller_profiles_persisted s
    ON s.userId = p.sellerId;

SELECT * FROM dim_product_price_band LIMIT 10;

--------------------------------------------------------------------------------
--- D4. DIM_PRODUCT_AFFINITY
--- Neo4j co-purchase graph enriched with MongoDB product metadata
--- Sources: neo4j_bought_with_persisted, mg_products_persisted
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dim_product_affinity AS
SELECT
    bw.product1Id               AS product_1_id,
    bw.product1Name             AS product_1_name,
    bw.product2Id               AS product_2_id,
    bw.product2Name             AS product_2_name,
    bw.coPurchaseCount          AS co_purchase_count,
    p1.productType              AS product_1_type,
    p2.productType              AS product_2_type,
    p1.sellerId                 AS product_1_seller_id,
    p2.sellerId                 AS product_2_seller_id,
    CASE
        WHEN p1.productType = p2.productType THEN 'same_type'
        ELSE 'cross_type'
    END                         AS affinity_type,
    CASE
        WHEN p1.sellerId = p2.sellerId THEN 'same_seller'
        ELSE 'cross_seller'
    END                         AS affinity_seller
FROM neo4j_bought_with_persisted bw
LEFT JOIN mg_products_persisted p1 ON p1.id = bw.product1Id
LEFT JOIN mg_products_persisted p2 ON p2.id = bw.product2Id;

SELECT * FROM dim_product_affinity LIMIT 10;


================================================================================
--- LAYER 3: FACT VIEWS
--- Purpose: measurable events and transactions used directly by OLAP
================================================================================

--------------------------------------------------------------------------------
--- F1. FACTS_SUBSCRIPTION_BILLING
--- Subscription invoice revenue with tier and user keys
--- Sources: orcl_sub_invoices_persisted + orcl_sub_invoice_lines_persisted
---          + orcl_subscriptions_persisted + orcl_sub_tiers_persisted
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW facts_subscription_billing AS
SELECT
    i.id                        AS invoice_id,
    i.userId                    AS user_id,
    i.subscriptionId            AS subscription_id,
    i.status                    AS invoice_status,
    i.totalUsd                  AS total_usd,
    i.subtotalUsd               AS subtotal_usd,
    i.taxUsd                    AS tax_usd,
    i.discountUsd               AS discount_usd,
    i.billingPeriodStart        AS billing_period_start,
    i.billingPeriodEnd          AS billing_period_end,
    i.paidAt                    AS paid_at,
    l.id                        AS line_id,
    l.productId                 AS product_id,
    l.quantity,
    l.unitPriceUsd              AS unit_price_usd,
    l.lineTotalUsd              AS line_total_usd,
    s.tierId                    AS tier_id,
    t.name                      AS tier_name,
    s.billingCycle              AS billing_cycle
FROM orcl_sub_invoices_persisted i
LEFT JOIN orcl_sub_invoice_lines_persisted l
    ON l.invoiceId = i.id
JOIN orcl_subscriptions_persisted s
    ON s.id = i.subscriptionId
LEFT JOIN orcl_sub_tiers_persisted t
    ON t.id = s.tierId
WHERE i.status = 'paid';

SELECT * FROM facts_subscription_billing LIMIT 10;
================================================================================
--- LAYER 4: OLAP ANALYTICAL VIEWS
--- 12 views covering:
---   ROLLUP, CUBE, GROUPING SETS,
---   Window functions (SUM/LAG/LEAD/RANK/DENSE_RANK/PERCENT_RANK/
---                     ROW_NUMBER/FIRST_VALUE/LAST_VALUE/NTILE),
---   PIVOT, UNPIVOT,
---   Statistical functions (PERCENTILE_CONT, STDDEV, VARIANCE, MEDIAN)
================================================================================
--------------------------------------------------------------------------------
--- OLAP-1. OLAP_PRODUCT_TYPE_COUNTRY_CUBE
--- Revenue and quantity by product type and shipping country
--- Technique: GROUP BY CUBE — all combinations including subtotals
--- Sources: facts_order_revenue
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_product_type_country_cube AS
SELECT
    COALESCE(product_type, '** ALL TYPES **')      AS product_type,
    COALESCE(shipping_country, '** ALL COUNTRIES **') AS shipping_country,
    COUNT(DISTINCT order_id)                        AS total_orders

FROM facts_order_revenue
GROUP BY CUBE(product_type, shipping_country)
ORDER BY 1 DESC, 2 DESC;

SELECT * FROM olap_product_type_country_cube;

--------------------------------------------------------------------------------
--- OLAP-2. OLAP_SUBSCRIPTION_BILLING_TREND
--- Monthly subscription revenue per tier with window analytics
--- Technique: GROUP BY + window functions
---   SUM OVER (cumulative revenue per tier)
---   LAG (previous month revenue)
---   LEAD (next month revenue)
---   FIRST_VALUE / LAST_VALUE (baseline and latest revenue)
---   MoM growth %
--- Sources: facts_subscription_billing
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_subscription_billing_trend AS
WITH monthly AS (
    SELECT
        tier_name,
        billing_cycle,
        date_format(CAST(billing_period_start AS DATE), 'yyyy-MM') AS billing_month,
        COUNT(DISTINCT invoice_id)                  AS invoice_count,
        COUNT(DISTINCT user_id)                     AS unique_users,
        ROUND(SUM(total_usd), 2)                    AS total_billed_usd,
        ROUND(SUM(discount_usd), 2)                 AS total_discount_usd,
        ROUND(SUM(tax_usd), 2)                      AS total_tax_usd
    FROM facts_subscription_billing
    GROUP BY tier_name, billing_cycle,
             date_format(CAST(billing_period_start AS DATE), 'yyyy-MM')
)
SELECT
    tier_name,
    billing_cycle,
    billing_month,
    invoice_count,
    unique_users,
    total_billed_usd,
    total_discount_usd,
    total_tax_usd,
    SUM(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS UNBOUNDED PRECEDING
    )                                               AS cumulative_revenue_usd,
    LAG(total_billed_usd, 1) OVER (
        PARTITION BY tier_name ORDER BY billing_month
    )                                               AS prev_month_revenue_usd,
    LEAD(total_billed_usd, 1) OVER (
        PARTITION BY tier_name ORDER BY billing_month
    )                                               AS next_month_revenue_usd,
    FIRST_VALUE(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS UNBOUNDED PRECEDING
    )                                               AS first_month_revenue_usd,
    LAST_VALUE(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                               AS last_month_revenue_usd,
    ROUND(
        100.0 * (total_billed_usd - LAG(total_billed_usd, 1) OVER (
            PARTITION BY tier_name ORDER BY billing_month))
        / NULLIF(LAG(total_billed_usd, 1) OVER (
            PARTITION BY tier_name ORDER BY billing_month), 0),
    2)                                              AS mom_growth_pct
FROM monthly
ORDER BY tier_name, billing_month;

SELECT * FROM olap_subscription_billing_trend;

--------------------------------------------------------------------------------
--- OLAP-3. OLAP_COHORT_RETENTION_BY_TIER
--- Subscription cohort retention and churn analysis per tier
--- Technique: GROUP BY + window functions
---   SUM OVER (cumulative churn)
---   AVG OVER (rolling 3-month churn rate)
---   LEAD (next month projected churn)
--- Sources: dim_user_subscription_tier
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_cohort_retention_by_tier AS
WITH cohort_monthly AS (
    SELECT
        tier_name,
        cohort_month,
        COUNT(*)                AS cohort_size,
        SUM(churned_flag)       AS churned_count,
        SUM(retained_30d)       AS retained_30d,
        SUM(retained_90d)       AS retained_90d
    FROM dim_user_subscription_tier
    GROUP BY tier_name, cohort_month
),
with_windows AS (
    SELECT
        tier_name,
        cohort_month,
        cohort_size,
        churned_count,
        retained_30d,
        retained_90d,
        SUM(churned_count) OVER (
            PARTITION BY tier_name
            ORDER BY cohort_month
            ROWS UNBOUNDED PRECEDING
        )                                           AS cumulative_churned,
        ROUND(churned_count / NULLIF(cohort_size, 0) * 100, 2)
                                                    AS churn_rate_pct,
        ROUND(AVG(churned_count / NULLIF(cohort_size, 0) * 100) OVER (
            PARTITION BY tier_name
            ORDER BY cohort_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2)                                       AS rolling_3m_churn_rate,
        LEAD(ROUND(churned_count / NULLIF(cohort_size, 0) * 100, 2), 1)
            OVER (PARTITION BY tier_name ORDER BY cohort_month)
                                                    AS next_month_churn_proj
    FROM cohort_monthly
)
SELECT
    tier_name,
    cohort_month,
    cohort_size,
    churned_count,
    cumulative_churned,
    retained_30d,
    retained_90d,
    ROUND((cohort_size - cumulative_churned) / NULLIF(cohort_size, 0) * 100, 2)
                                                    AS survival_rate_pct,
    churn_rate_pct,
    rolling_3m_churn_rate,
    next_month_churn_proj
FROM with_windows
ORDER BY tier_name, cohort_month;

SELECT * FROM olap_cohort_retention_by_tier;

--------------------------------------------------------------------------------
--- OLAP-4. OLAP_EVENT_FUNNEL_BY_COUNTRY
--- Monthly user activity funnel by country with engagement metrics
--- Technique: GROUP BY ROLLUP (month → country) + window functions
---   SUM OVER (cumulative events per country)
---   LAG (previous month events for MoM delta)
--- Sources: facts_user_events
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_event_funnel_by_country AS
WITH monthly_activity AS (
    SELECT
        event_year_month                            AS activity_month,
        country_code,
        COUNT(*)                                    AS total_events,
        SUM(CASE WHEN event_type = 'page_view'      THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN event_type = 'product_view'   THEN 1 ELSE 0 END) AS product_views,
        SUM(CASE WHEN event_type = 'search'         THEN 1 ELSE 0 END) AS searches,
        SUM(CASE WHEN event_type = 'add_to_cart'    THEN 1 ELSE 0 END) AS add_to_cart,
        SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) AS checkout_starts,
        SUM(CASE WHEN event_type = 'purchase'       THEN 1 ELSE 0 END) AS purchases,
        GROUPING(event_year_month)                  AS grp_month,
        GROUPING(country_code)                      AS grp_country
    FROM facts_user_events
    GROUP BY ROLLUP(event_year_month, country_code)
)
SELECT
    COALESCE(activity_month, '{Grand Total}')       AS activity_month,
    COALESCE(country_code, CASE WHEN grp_country = 1 AND grp_month = 0
        THEN 'Subtotal: ' || activity_month ELSE '** ALL **' END)
                                                    AS country_code,
    total_events,
    page_views,
    product_views,
    searches,
    add_to_cart,
    checkout_starts,
    purchases,
    ROUND(purchases / NULLIF(add_to_cart, 0) * 100, 2)
                                                    AS cart_to_purchase_pct,
    SUM(total_events) OVER (
        PARTITION BY country_code
        ORDER BY activity_month
        ROWS UNBOUNDED PRECEDING
    )                                               AS cumulative_events,
    LAG(total_events, 1) OVER (
        PARTITION BY country_code ORDER BY activity_month
    )                                               AS prev_month_events
FROM monthly_activity
ORDER BY 1, 2;

SELECT * FROM olap_event_funnel_by_country;
--------------------------------------------------------------------------------
--- OLAP-5. OLAP_PRODUCT_AFFINITY_ANALYSIS
--- Co-purchase affinity ranking by type and seller pairing
--- Technique: GROUPING SETS + RANK + SUM OVER (running total)
--- Sources: dim_product_affinity
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_product_affinity_analysis AS
WITH affinity_grouped AS (
    SELECT
        affinity_type,
        affinity_seller,
        product_1_id,
        product_2_id,
        product_1_type,
        product_2_type,
        SUM(co_purchase_count)                      AS total_co_purchases,
        COUNT(*)                                    AS pair_count,
        GROUPING(product_1_id)                      AS is_subtotal
    FROM dim_product_affinity
    GROUP BY GROUPING SETS (
        (affinity_type, affinity_seller, product_1_id, product_2_id,
         product_1_type, product_2_type),
        (affinity_type),
        (affinity_seller)
    )
)
SELECT
    COALESCE(affinity_type,   '** ALL **')          AS affinity_type,
    COALESCE(affinity_seller, '** ALL **')          AS affinity_seller,
    CASE WHEN is_subtotal = 1 THEN NULL ELSE product_1_id END AS product_1_id,
    CASE WHEN is_subtotal = 1 THEN NULL ELSE product_2_id END AS product_2_id,
    pair_count,
    total_co_purchases,
    CASE WHEN is_subtotal = 0
        THEN RANK() OVER (PARTITION BY affinity_type ORDER BY total_co_purchases DESC)
    END                                             AS rank_in_type,
    CASE WHEN is_subtotal = 0
        THEN SUM(total_co_purchases) OVER (
            PARTITION BY affinity_seller
            ORDER BY total_co_purchases DESC
            ROWS UNBOUNDED PRECEDING)
    END                                             AS running_co_purchases,
    CASE WHEN is_subtotal = 1 THEN 'Subtotal'
         WHEN total_co_purchases >= AVG(total_co_purchases) OVER () THEN 'above_avg'
         ELSE 'below_avg'
    END                                             AS vs_avg_flag
FROM affinity_grouped
ORDER BY affinity_type, total_co_purchases DESC;

SELECT * FROM olap_product_affinity_analysis;
--------------------------------------------------------------------------------
--- OLAP-6. OLAP_EVENT_TYPE_WINDOW_ANALYTICS
--- Event counts with multiple window frames over time
--- Technique: window functions — all major frame types
---   SUM OVER UNBOUNDED PRECEDING (cumulative)
---   SUM OVER CURRENT ROW AND UNBOUNDED FOLLOWING (trailing)
---   SUM OVER 1 PRECEDING (rolling 2-period)
---   SUM OVER BETWEEN 1 PRECEDING AND 1 FOLLOWING (centred 3-period)
---   FIRST_VALUE / LAST_VALUE
--- Sources: facts_user_events
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW olap_event_type_window_analytics AS
WITH monthly_events AS (
    SELECT
        event_type,
        event_year_month,
        COUNT(*) AS event_count
    FROM facts_user_events
    GROUP BY event_type, event_year_month
)
SELECT
    event_type,
    event_year_month,
    event_count,
    SUM(event_count) OVER (
        PARTITION BY event_type
        ORDER BY event_year_month
        ROWS UNBOUNDED PRECEDING
    )                                               AS cumulative_events,
    SUM(event_count) OVER (
        PARTITION BY event_type
        ORDER BY event_year_month
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )                                               AS trailing_events,
    SUM(event_count) OVER (
        PARTITION BY event_type
        ORDER BY event_year_month
        ROWS 1 PRECEDING
    )                                               AS rolling_2m_events,
    SUM(event_count) OVER (
        PARTITION BY event_type
        ORDER BY event_year_month
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    )                                               AS centred_3m_events,
    FIRST_VALUE(event_count) OVER (
        PARTITION BY event_type
        ORDER BY event_year_month
        ROWS UNBOUNDED PRECEDING
    )                                               AS first_month_count,
    LAST_VALUE(event_count) OVER (
        PARTITION BY event_type
        ORDER BY event_year_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                               AS last_month_count,
    LAG(event_count, 1)  OVER (
        PARTITION BY event_type ORDER BY event_year_month
    )                                               AS prev_month_count,
    LEAD(event_count, 1) OVER (
        PARTITION BY event_type ORDER BY event_year_month
    )                                               AS next_month_count
FROM monthly_events
ORDER BY event_type, event_year_month;

SELECT * FROM olap_event_type_window_analytics;