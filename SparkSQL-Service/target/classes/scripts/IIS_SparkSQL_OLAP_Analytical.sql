--------------------------------------------------------------------------------
-- IIS_SparkSQL_OLAP_Analytical.sql
-- ROLAP Multidimensional Analytical Layer
--
-- PREREQUISITE: Run all DS* scripts first to register the base views:
--   DS1_PostgreSQL_SparkSQL_Views_from_REST.sql   → pg_orders_view, pg_order_items_view, ...
--   DS2_Oracle_SparkSQL_Views_from_REST.sql       → orcl_users_view, orcl_subscriptions_view, ...
--   DS3_TimescaleDB_SparkSQL_Views_from_REST.sql  → ts_events_view
--   DS4_MongoDB_SparkSQL_Views_from_REST.sql      → mongo_products_view, ...
--
-- STRUCTURE:
--   1. Dimension Views   → OLAP_DIM_*
--   2. Fact View         → OLAP_FACTS_*
--   3. Analytical Views  → OLAP_VIEW_* (ROLLUP / CUBE aggregations)
--
-- Adjust column names to match your actual schema once the base views are verified.
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- ══ DIMENSION VIEWS ══════════════════════════════════════════════════════════
--------------------------------------------------------------------------------

-- ── DIM 1: Users + Subscriptions + Tiers (Oracle) ────────────────────────────
-- Maps each user to their current subscription tier

CREATE OR REPLACE VIEW OLAP_DIM_USERS_SUBSCRIPTIONS AS
SELECT
    u.user_id,
    u.username,
    u.email,
    u.country,
    u.created_at                        AS user_created_at,
    s.subscription_id,
    s.tier_id,
    s.status                            AS subscription_status,
    s.start_date,
    s.end_date,
    t.tier_name,
    t.tier_level,
    p.monthly_price,
    p.currency
FROM orcl_users_view            u
LEFT JOIN orcl_subscriptions_view   s ON u.user_id     = s.user_id
LEFT JOIN orcl_sub_tiers_view       t ON s.tier_id     = t.tier_id
LEFT JOIN orcl_tier_pricing_view    p ON t.tier_id     = p.tier_id;

SELECT * FROM OLAP_DIM_USERS_SUBSCRIPTIONS LIMIT 5;

--------------------------------------------------------------------------------
-- ── DIM 2: Calendar (derived from Orders dates) ───────────────────────────────
-- Standard date dimension — year / quarter / month / week / day

CREATE OR REPLACE VIEW OLAP_DIM_CALENDAR AS
SELECT DISTINCT
    CAST(order_date AS DATE)                        AS cal_date,
    YEAR(CAST(order_date AS DATE))                  AS cal_year,
    QUARTER(CAST(order_date AS DATE))               AS cal_quarter,
    MONTH(CAST(order_date AS DATE))                 AS cal_month,
    DATE_FORMAT(CAST(order_date AS DATE), 'MMMM')   AS cal_month_name,
    WEEKOFYEAR(CAST(order_date AS DATE))            AS cal_week,
    DAYOFMONTH(CAST(order_date AS DATE))            AS cal_day,
    DAYOFWEEK(CAST(order_date AS DATE))             AS cal_day_of_week,
    DATE_FORMAT(CAST(order_date AS DATE), 'EEEE')   AS cal_day_name
FROM pg_orders_view
WHERE order_date IS NOT NULL;

SELECT * FROM OLAP_DIM_CALENDAR ORDER BY cal_date LIMIT 10;

--------------------------------------------------------------------------------
-- ── DIM 3: Product / Tier Pricing (Oracle tiers as product dimension) ─────────

CREATE OR REPLACE VIEW OLAP_DIM_PRODUCTS AS
SELECT
    t.tier_id           AS product_id,
    t.tier_name         AS product_name,
    t.tier_level        AS product_category,
    p.monthly_price     AS unit_price,
    p.currency
FROM orcl_sub_tiers_view    t
LEFT JOIN orcl_tier_pricing_view p ON t.tier_id = p.tier_id;

SELECT * FROM OLAP_DIM_PRODUCTS;

--------------------------------------------------------------------------------
-- ── DIM 4: Event Types (TimescaleDB) ─────────────────────────────────────────

CREATE OR REPLACE VIEW OLAP_DIM_EVENT_TYPES AS
SELECT DISTINCT
    eventType               AS event_type,
    COUNT(*) OVER (PARTITION BY eventType) AS event_count_total
FROM ts_events_view
WHERE eventType IS NOT NULL;

SELECT * FROM OLAP_DIM_EVENT_TYPES ORDER BY event_count_total DESC;

--------------------------------------------------------------------------------
-- ══ FACT VIEWS ════════════════════════════════════════════════════════════════
--------------------------------------------------------------------------------

-- ── FACT 1: Order Revenue Fact ────────────────────────────────────────────────
-- Grain: one row per order line item

CREATE OR REPLACE VIEW OLAP_FACTS_ORDER_REVENUE AS
SELECT
    o.order_id,
    o.user_id,
    CAST(o.order_date AS DATE)      AS order_date,
    YEAR(CAST(o.order_date AS DATE))    AS order_year,
    MONTH(CAST(o.order_date AS DATE))   AS order_month,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    CAST(oi.quantity AS DOUBLE) * CAST(oi.unit_price AS DOUBLE) AS line_revenue,
    o.status                        AS order_status
FROM pg_orders_view     o
JOIN pg_order_items_view oi ON o.order_id = oi.order_id;

SELECT * FROM OLAP_FACTS_ORDER_REVENUE LIMIT 10;
SELECT COUNT(*) AS total_fact_rows FROM OLAP_FACTS_ORDER_REVENUE;

--------------------------------------------------------------------------------
-- ── FACT 2: Subscription Invoice Fact (Oracle) ────────────────────────────────
-- Grain: one row per subscription invoice line

CREATE OR REPLACE VIEW OLAP_FACTS_SUBSCRIPTION_REVENUE AS
SELECT
    i.invoice_id,
    i.subscription_id,
    s.user_id,
    s.tier_id,
    CAST(i.invoice_date AS DATE)        AS invoice_date,
    YEAR(CAST(i.invoice_date AS DATE))  AS invoice_year,
    MONTH(CAST(i.invoice_date AS DATE)) AS invoice_month,
    il.line_amount,
    i.total_amount,
    i.status                            AS invoice_status
FROM orcl_sub_invoices_view     i
JOIN orcl_sub_invoice_lines_view il ON i.invoice_id     = il.invoice_id
JOIN orcl_subscriptions_view     s  ON i.subscription_id = s.subscription_id;

SELECT * FROM OLAP_FACTS_SUBSCRIPTION_REVENUE LIMIT 10;

--------------------------------------------------------------------------------
-- ══ ANALYTICAL VIEWS (ROLLUP / CUBE) ══════════════════════════════════════════
--------------------------------------------------------------------------------

-- ── OLAP 1: Revenue by Subscription Tier ─────────────────────────────────────
-- ROLLUP: tier_level → tier_name → month → subtotals + grand total

CREATE OR REPLACE VIEW OLAP_VIEW_REVENUE_BY_TIER AS
SELECT
    t.tier_level,
    t.tier_name,
    f.invoice_year,
    f.invoice_month,
    COUNT(DISTINCT f.invoice_id)        AS invoice_count,
    SUM(f.total_amount)                 AS total_revenue,
    AVG(f.total_amount)                 AS avg_invoice_value
FROM OLAP_FACTS_SUBSCRIPTION_REVENUE f
JOIN OLAP_DIM_PRODUCTS t ON f.tier_id = t.product_id
GROUP BY ROLLUP(t.tier_level, t.tier_name, f.invoice_year, f.invoice_month);

SELECT * FROM OLAP_VIEW_REVENUE_BY_TIER ORDER BY tier_level, tier_name, invoice_year, invoice_month;

--------------------------------------------------------------------------------
-- ── OLAP 2: Order Revenue by Calendar ────────────────────────────────────────
-- ROLLUP: year → quarter → month → subtotals

CREATE OR REPLACE VIEW OLAP_VIEW_REVENUE_BY_CALENDAR AS
SELECT
    order_year,
    order_month,
    COUNT(DISTINCT order_id)            AS order_count,
    SUM(line_revenue)                   AS total_revenue,
    AVG(line_revenue)                   AS avg_line_value,
    SUM(quantity)                       AS total_units_sold
FROM OLAP_FACTS_ORDER_REVENUE
GROUP BY ROLLUP(order_year, order_month);

SELECT * FROM OLAP_VIEW_REVENUE_BY_CALENDAR ORDER BY order_year, order_month;

--------------------------------------------------------------------------------
-- ── OLAP 3: Events by Type and Time ──────────────────────────────────────────
-- CUBE: event_type × year × month (all combinations + subtotals)

CREATE OR REPLACE VIEW OLAP_VIEW_EVENTS_BY_TYPE AS
SELECT
    eventType                               AS event_type,
    YEAR(CAST(occurredAt AS TIMESTAMP))     AS event_year,
    MONTH(CAST(occurredAt AS TIMESTAMP))    AS event_month,
    COUNT(*)                                AS event_count
FROM ts_events_view
WHERE eventType IS NOT NULL
GROUP BY CUBE(eventType,
              YEAR(CAST(occurredAt AS TIMESTAMP)),
              MONTH(CAST(occurredAt AS TIMESTAMP)));

SELECT * FROM OLAP_VIEW_EVENTS_BY_TYPE
ORDER BY event_year, event_month, event_count DESC
LIMIT 50;

--------------------------------------------------------------------------------
-- ── OLAP 4: Cross-source — User Subscriptions + Order Activity ───────────────
-- Joins Oracle (subscriptions) with PostgreSQL (orders) for customer 360 view
-- ROLLUP: tier → user → month

CREATE OR REPLACE VIEW OLAP_VIEW_USER_ACTIVITY AS
SELECT
    us.tier_name,
    us.tier_level,
    us.user_id,
    us.username,
    f.order_year,
    f.order_month,
    COUNT(DISTINCT f.order_id)          AS orders_placed,
    SUM(f.line_revenue)                 AS marketplace_spend,
    us.monthly_price                    AS subscription_price
FROM OLAP_DIM_USERS_SUBSCRIPTIONS us
LEFT JOIN OLAP_FACTS_ORDER_REVENUE f ON us.user_id = f.user_id
GROUP BY ROLLUP(
    us.tier_level,
    us.tier_name,
    us.user_id,
    us.username,
    us.monthly_price,
    f.order_year,
    f.order_month
);

SELECT * FROM OLAP_VIEW_USER_ACTIVITY
ORDER BY tier_level, tier_name, user_id, order_year, order_month
LIMIT 100;

--------------------------------------------------------------------------------
-- ══ REST ENDPOINT VERIFICATION ════════════════════════════════════════════════
-- After running this script, these URLs should return data:
--
-- http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_REVENUE_BY_TIER
-- http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_REVENUE_BY_CALENDAR
-- http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_EVENTS_BY_TYPE
-- http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_USER_ACTIVITY
--
-- Paginated (recommended for OLAP views with subtotal rows):
-- http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_REVENUE_BY_TIER?limit=500
-- http://localhost:9990/IIS-SparkSQL-Service/rest/view/OLAP_VIEW_EVENTS_BY_TYPE?limit=500
--
-- Schema inspection:
-- http://localhost:9990/IIS-SparkSQL-Service/rest/STRUCT/OLAP_FACTS_ORDER_REVENUE
-- http://localhost:9990/IIS-SparkSQL-Service/rest/STRUCT/OLAP_DIM_USERS_SUBSCRIPTIONS
--------------------------------------------------------------------------------
