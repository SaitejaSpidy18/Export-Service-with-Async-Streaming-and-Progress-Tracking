-- File: seeds/init.sql
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    signup_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    country_code CHAR(2) NOT NULL,
    subscription_tier VARCHAR(50) DEFAULT 'free',
    lifetime_value NUMERIC(10, 2) DEFAULT 0.00
);
CREATE INDEX IF NOT EXISTS idx_users_country_code ON users(country_code);
CREATE INDEX IF NOT EXISTS idx_users_subscription_tier ON users(subscription_tier);
CREATE INDEX IF NOT EXISTS idx_users_lifetime_value ON users(lifetime_value);
CREATE TABLE IF NOT EXISTS export_jobs (
    id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    filter_country_code CHAR(2),
    filter_subscription_tier VARCHAR(50),
    filter_min_ltv NUMERIC(10,2),
    columns TEXT,
    delimiter CHAR(1) DEFAULT ',',
    quote_char CHAR(1) DEFAULT '"',
    total_rows BIGINT DEFAULT 0,
    processed_rows BIGINT DEFAULT 0,
    error TEXT,
    file_path TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);
