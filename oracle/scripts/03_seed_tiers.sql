-- ============================================================
--  IIS Project — Oracle XE 21c
--  Script 03: Seed subscription tiers and pricing
--
--  Run as: FDBO, connected to XEPDB1
--  Run this AFTER 02_create_tables.sql and 02b_alter_is_active.sql
--  Run this BEFORE run_sqlldr.ps1 (subscriptions.csv references tier_id)
-- ============================================================


-- ── SUBSCRIPTION_TIERS ───────────────────────────────────────

INSERT INTO SUBSCRIPTION_TIERS (id, name, description, features) VALUES (
    1,
    'Free',
    'Basic software access, up to 5 marketplace purchases/year',
    '{"seats": 1, "api_access": false, "priority_support": false, "marketplace_purchases_per_year": 5, "apps": {"CanvasEditor": "free", "VideoSuite": null}}'
);

INSERT INTO SUBSCRIPTION_TIERS (id, name, description, features) VALUES (
    2,
    'Pro',
    'Full software, unlimited purchases, early access',
    '{"seats": 1, "api_access": false, "priority_support": false, "marketplace_purchases_per_year": -1, "apps": {"CanvasEditor": "premium", "VideoSuite": "standard"}}'
);

INSERT INTO SUBSCRIPTION_TIERS (id, name, description, features) VALUES (
    3,
    'Business',
    'Everything in Pro plus team seats, API access, priority support',
    '{"seats": 10, "api_access": true, "priority_support": true, "marketplace_purchases_per_year": -1, "apps": {"CanvasEditor": "premium", "VideoSuite": "premium"}}'
);


-- ── SUBSCRIPTION_TIER_PRICING ────────────────────────────────
-- Pro and Business had a price increase in June 2024.
-- Rows with valid_to = NULL are currently active.

-- Free tier (always 0.00, no price change)
INSERT INTO SUBSCRIPTION_TIER_PRICING (tier_id, valid_from, valid_to, monthly_price_usd, is_active) VALUES (
    1,
    TIMESTAMP '2023-01-01 00:00:00 +00:00',
    NULL,
    0.00,
    1
);

-- Pro — old price (before June 2024)
INSERT INTO SUBSCRIPTION_TIER_PRICING (tier_id, valid_from, valid_to, monthly_price_usd, is_active) VALUES (
    2,
    TIMESTAMP '2023-01-01 00:00:00 +00:00',
    TIMESTAMP '2024-06-01 00:00:00 +00:00',
    14.99,
    0
);

-- Pro — current price (from June 2024)
INSERT INTO SUBSCRIPTION_TIER_PRICING (tier_id, valid_from, valid_to, monthly_price_usd, is_active) VALUES (
    2,
    TIMESTAMP '2024-06-01 00:00:00 +00:00',
    NULL,
    19.99,
    1
);

-- Business — old price (before June 2024)
INSERT INTO SUBSCRIPTION_TIER_PRICING (tier_id, valid_from, valid_to, monthly_price_usd, is_active) VALUES (
    3,
    TIMESTAMP '2023-01-01 00:00:00 +00:00',
    TIMESTAMP '2024-06-01 00:00:00 +00:00',
    39.99,
    0
);

-- Business — current price (from June 2024)
INSERT INTO SUBSCRIPTION_TIER_PRICING (tier_id, valid_from, valid_to, monthly_price_usd, is_active) VALUES (
    3,
    TIMESTAMP '2024-06-01 00:00:00 +00:00',
    NULL,
    49.99,
    1
);

COMMIT;


-- ── Verify ───────────────────────────────────────────────────
SELECT t.id, t.name, p.monthly_price_usd, p.valid_from, p.valid_to, p.is_active
FROM SUBSCRIPTION_TIERS t
JOIN SUBSCRIPTION_TIER_PRICING p ON p.tier_id = t.id
ORDER BY t.id, p.valid_from;
