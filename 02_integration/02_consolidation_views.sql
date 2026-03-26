-- ============================================================
-- CONSOLIDATION VIEWS
--
-- Purpose:
--   This layer integrates and normalizes data from multiple sources,
--   including Oracle, PostgreSQL, TimescaleDB, MongoDB, and Neo4j.
--
-- Description:
--   Consolidation views (V_CONS_*) provide a unified relational model
--   over heterogeneous data sources. They standardize structure, naming,
--   and data types in order to support downstream analytical processing.
--
--   This layer contains:
--   - descriptive master data (e.g. users, products, tiers)
--   - normalized transactional data (e.g. orders, invoices)
--   - integrated cross-source views used as the foundation for analytics
--
-- Notes:
--   - Some views are limited to 10000 rows due to the external access layer.
--   - These views are not final analytical objects, but serve as a base
--     for fact views and dimension views.
-- ============================================================


-- 01. V_CONS_USERS
-- Description:
--   Central Oracle user master view.
-- Can be used for:
--   - user counts
--   - active vs inactive users
--   - user growth over time
--   - country / city analysis
--   - joining users to subscriptions, orders, invoices, events
CREATE OR REPLACE VIEW FDBO.V_CONS_USERS AS
SELECT
    u.id           AS user_id,
    u.email        AS user_email,
    u.full_name    AS user_full_name,
    u.country_code AS user_country_code,
    u.city         AS user_city,
    u.created_at   AS user_created_at,
    u.last_login_at AS user_last_login_at,
    u.is_active    AS user_is_active
FROM FDBO.USERS u;


-- 02. V_CONS_SUBSCRIPTIONS
-- Description:
--   Core Oracle subscription view.
-- Can be used for:
--   - active vs cancelled subscriptions
--   - billing cycle analysis
--   - subscription lifecycle analysis
--   - retention/churn preparation
CREATE OR REPLACE VIEW FDBO.V_CONS_SUBSCRIPTIONS AS
SELECT
    s.id                   AS subscription_id,
    s.user_id              AS user_id,
    s.tier_id              AS tier_id,
    s.status               AS subscription_status,
    s.started_at           AS started_at,
    s.current_period_start AS current_period_start,
    s.current_period_end   AS current_period_end,
    s.cancelled_at         AS cancelled_at,
    s.cancel_reason        AS cancel_reason,
    s.billing_cycle        AS billing_cycle,
    s.created_at           AS subscription_created_at,
    s.updated_at           AS subscription_updated_at
FROM FDBO.SUBSCRIPTIONS s;


-- 03. V_CONS_SUBSCRIPTION_TIERS
-- Description:
--   Oracle subscription tier dictionary.
-- Can be used for:
--   - grouping subscriptions by tier
--   - tier portfolio analysis
CREATE OR REPLACE VIEW FDBO.V_CONS_SUBSCRIPTION_TIERS AS
SELECT
    st.id          AS tier_id,
    st.name        AS tier_name,
    st.description AS tier_description
FROM FDBO.SUBSCRIPTION_TIERS st;


-- 04. V_CONS_TIER_PRICING
-- Description:
--   Historical Oracle pricing dictionary.
-- Can be used for:
--   - historical pricing analysis
--   - monthly subscription value analysis
--   - active/inactive pricing records
CREATE OR REPLACE VIEW FDBO.V_CONS_TIER_PRICING AS
SELECT
    stp.id                AS pricing_id,
    stp.tier_id           AS tier_id,
    stp.valid_from        AS valid_from,
    stp.valid_to          AS valid_to,
    stp.monthly_price_usd AS monthly_price_usd,
    stp.is_active         AS pricing_is_active
FROM FDBO.SUBSCRIPTION_TIER_PRICING stp;


-- 05. V_CONS_SUB_INVOICES
-- Description:
--   Oracle subscription invoices normalized.
-- Can be used for:
--   - subscription revenue analysis
--   - invoice status analysis
--   - due vs paid invoice analysis
CREATE OR REPLACE FORCE VIEW FDBO.V_CONS_SUB_INVOICES AS
SELECT
    si.id AS subscription_invoice_id,
    si.user_id AS user_id,
    si.invoice_type AS invoice_type,
    si.status AS invoice_status,
    si.subtotal_usd AS subtotal_usd,
    si.tax_usd AS tax_usd,
    si.discount_usd AS discount_usd,
    si.total_usd AS total_usd,
    si.subscription_id AS subscription_id,
    si.billing_period_start AS billing_period_start,
    si.billing_period_end AS billing_period_end,
    si.paid_at AS paid_at,
    si.due_at AS due_at,
    si.created_at AS invoice_created_at
FROM FDBO.SUBSCRIPTION_INVOICES si
/


-- 06. V_CONS_SUB_INVOICE_LINES
-- Description:
--   Oracle subscription invoice line items normalized.
-- Can be used for:
--   - line-level revenue analysis
--   - product-related invoicing analysis
CREATE OR REPLACE VIEW FDBO.V_CONS_SUB_INVOICE_LINES AS
SELECT
    sil.id AS subscription_invoice_line_id,
    sil.invoice_id AS subscription_invoice_id,
    sil.product_id AS product_id,
    sil.description AS line_description,
    sil.quantity AS quantity,
    sil.unit_price_usd AS unit_price_usd,
    sil.line_total_usd AS line_total_usd,
    sil.created_at AS line_created_at
FROM FDBO.SUBSCRIPTION_INVOICE_LINES sil;
/


-- 07. V_CONS_SELLERS
-- Description:
--   Seller profiles from CSV external table.
-- Can be used for:
--   - seller geography
--   - verified vs unverified sellers
--   - seller-level rollups
CREATE OR REPLACE VIEW FDBO.V_CONS_SELLERS AS
SELECT
    esp.user_id AS seller_id,
    esp.display_name AS seller_display_name,
    esp.legal_name AS seller_legal_name,
    esp.tax_id AS seller_tax_id,
    esp.payout_email AS seller_payout_email,
    esp.country_code AS seller_country_code,
    esp.is_verified AS seller_is_verified,
    esp.bio AS seller_bio,
    esp.created_at AS seller_created_at,
    esp.updated_at AS seller_updated_at
FROM FDBO.EXT_SELLER_PROFILES esp;
/


-- 08. V_CONS_PG_ORDERS
-- Description:
--   PostgreSQL order headers normalized in Oracle.
-- Can be used for:
--   - order counts
--   - order status analysis
--   - shipping country analysis
--   - purchase timeline analysis
CREATE OR REPLACE VIEW FDBO.V_CONS_PG_ORDERS AS
SELECT
    o.id AS order_id,
    o.user_id AS user_id,
    o.invoice_id AS marketplace_invoice_id,
    o.status AS order_status,
    o.shipping_country AS shipping_country,
    TO_TIMESTAMP_TZ(o.created_at, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') AS order_created_at
FROM FDBO.V_PG_ORDERS o;
/


-- 09. V_CONS_PG_ORDER_ITEMS
-- Description:
--   PostgreSQL order item lines normalized in Oracle.
-- Can be used for:
--   - line-level sales analysis
--   - quantity analysis
--   - product purchase analysis
CREATE OR REPLACE VIEW FDBO.V_CONS_PG_ORDER_ITEMS AS
SELECT
    oi.id AS order_item_id,
    oi.order_id AS order_id,
    oi.product_id AS product_id,
    oi.quantity AS quantity,
    oi.unit_price_usd AS unit_price_usd,
    oi.line_total_usd AS line_total_usd
FROM FDBO.V_PG_ORDER_ITEMS oi;
/


-- 10. V_CONS_PRODUCTS
-- Description:
--   MongoDB product catalog normalized in Oracle.
-- Can be used for:
--   - product portfolio analysis
--   - active vs inactive products
--   - seller/product analysis
--   - event/order enrichment
CREATE OR REPLACE VIEW FDBO.V_CONS_PRODUCTS AS
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.product_type AS product_type,
    p.price_usd AS product_price_usd,
    p.seller_id AS seller_id,
    p.is_active AS product_is_active
FROM FDBO.V_MG_PRODUCTS p;
/


-- 11. V_CONS_USER_ACTIVITY
-- Description:
--   Oracle users + Timescale events + Mongo products.
-- Can be used for:
--   - behavioural analysis
--   - product interaction analysis
--   - event-type trend analysis
--   - pre-purchase activity analysis
--   - funnel preparation
CREATE OR REPLACE VIEW FDBO.V_CONS_USER_ACTIVITY AS
SELECT
    u.user_id,
    u.user_email,
    u.user_full_name,
    u.user_country_code,
    u.user_city,
    u.user_created_at,
    u.user_last_login_at,
    u.user_is_active,
    e.id AS event_id,
    e.event_type AS event_type,
    e.product_id AS product_id,
    e.metadata AS metadata,
    TO_TIMESTAMP_TZ(e.occurred_at, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') AS occurred_at,
    p.name AS product_name,
    p.product_type AS product_type,
    p.seller_id AS seller_id,
    p.price_usd AS product_price_usd,
    p.is_active AS product_is_active
FROM FDBO.V_CONS_USERS u
JOIN FDBO.V_TS_EVENTS e
    ON e.user_id = u.user_id
LEFT JOIN FDBO.V_MG_PRODUCTS p
    ON p.id = e.product_id;
/


