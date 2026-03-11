-- File: seeds/seed_users.sql
-- Populate the users table with exactly 10,000,000 rows using generate_series.
-- This will run once when the Postgres data directory is empty.
INSERT INTO users (name, email, signup_date, country_code, subscription_tier, lifetime_value)
SELECT
    'User ' || g.id AS name,
    'user' || g.id || '@example.com' AS email,
    NOW() - (g.id % 365) * INTERVAL '1 day' AS signup_date,
    (ARRAY['US','IN','GB','DE','FR','CA','AU','BR','JP','SG'])[(random()*9)::int + 1] AS country_code,
    (ARRAY['free','basic','premium'])[(random()*2)::int + 1] AS subscription_tier,
    round((random()*1000)::numeric, 2) AS lifetime_value
FROM generate_series(1, 10000000) AS g(id);
