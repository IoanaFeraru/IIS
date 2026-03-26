-- Note:Dimension views help separate descriptive attributes (dimensions) from transactional data (facts)

-- ============================================================
-- 01. V_DIM_USERS
-- Description:
--   User dimension view providing descriptive user attributes.
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_DIM_USERS AS
SELECT
    user_id,
    user_email,
    user_full_name,
    user_country_code,
    user_city,
    user_created_at,
    user_last_login_at,
    user_is_active
FROM FDBO.V_CONS_USERS;
/

-- ============================================================
-- 02. V_DIM_SUBSCRIPTION_TIERS
-- Description:
--   Subscription tier dimension view.
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_DIM_SUBSCRIPTION_TIERS AS
SELECT
    tier_id,
    tier_name,
    tier_description
FROM FDBO.V_CONS_SUBSCRIPTION_TIERS;
/

-- ============================================================
-- 03. V_DIM_TIME
-- Description:
--   Time dimension derived from multiple date sources.
--   Enables analysis by year, month, quarter.
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_DIM_TIME AS
SELECT DISTINCT
    TRUNC(dt, 'DD') AS date_key,
    EXTRACT(YEAR FROM dt) AS year_no,
    EXTRACT(MONTH FROM dt) AS month_no,
    TO_CHAR(dt, 'YYYY-MM') AS year_month,
    TO_CHAR(dt, 'MON') AS month_name,
    TO_CHAR(dt, 'Q') AS quarter_no
FROM (
    SELECT CAST(user_created_at AS DATE) AS dt FROM FDBO.V_CONS_USERS
    UNION
    SELECT CAST(started_at AS DATE) AS dt FROM FDBO.V_CONS_SUBSCRIPTIONS
    UNION
    SELECT CAST(invoice_created_at AS DATE) AS dt FROM FDBO.V_CONS_SUB_INVOICES
    UNION
    SELECT CAST(order_created_at AS DATE) AS dt FROM FDBO.V_CONS_PG_ORDERS
);
/

-- ============================================================
-- 04. V_DIM_ORDER_STATUS
-- Description:
--   Order status dimension.
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_DIM_ORDER_STATUS AS
SELECT DISTINCT
    order_status
FROM FDBO.V_CONS_PG_ORDERS;
/

-- ============================================================
-- 05. V_DIM_INVOICE_STATUS
-- Description:
--   Invoice status dimension.
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_DIM_INVOICE_STATUS AS
SELECT DISTINCT
    invoice_status
FROM FDBO.V_CONS_SUB_INVOICES;
/

-- ============================================================
-- 06. V_DIM_SUBSCRIPTION_STATUS
-- Description:
--   Subscription status dimension.
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_DIM_SUBSCRIPTION_STATUS AS
SELECT DISTINCT
    subscription_status
FROM FDBO.V_CONS_SUBSCRIPTIONS;
/