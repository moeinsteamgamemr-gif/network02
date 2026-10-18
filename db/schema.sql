CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS migrations (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(128) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_sessions (
    id BIGSERIAL PRIMARY KEY,
    admin_id BIGINT NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
    token_hash VARCHAR(128) NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_sessions_token
ON admin_sessions(token_hash);

CREATE INDEX IF NOT EXISTS idx_admin_sessions_expiry
ON admin_sessions(expires_at);


CREATE TABLE IF NOT EXISTS services (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS gateways (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    worker_name VARCHAR(255) NOT NULL UNIQUE,
    worker_url TEXT,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    region VARCHAR(30) NOT NULL DEFAULT 'eu',
    status VARCHAR(30) NOT NULL DEFAULT 'creating',
    latency_ms INTEGER,
    last_seen TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gateways_enabled
ON gateways(enabled);

CREATE INDEX IF NOT EXISTS idx_gateways_region
ON gateways(region);

CREATE INDEX IF NOT EXISTS idx_gateways_status
ON gateways(status);


CREATE TABLE IF NOT EXISTS accounts (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(150) NOT NULL DEFAULT '',
    uuid UUID NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    note TEXT NOT NULL DEFAULT '',
    data_limit BIGINT NOT NULL DEFAULT 0 CHECK (data_limit >= 0),
    data_used BIGINT NOT NULL DEFAULT 0 CHECK (data_used >= 0),
    device_limit INTEGER NOT NULL DEFAULT 0 CHECK (device_limit >= 0),
    expires_at TIMESTAMPTZ,
    service_id BIGINT REFERENCES services(id) ON DELETE SET NULL,
    gateway_id BIGINT REFERENCES gateways(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accounts_enabled
ON accounts(enabled);

CREATE INDEX IF NOT EXISTS idx_accounts_service
ON accounts(service_id);

CREATE INDEX IF NOT EXISTS idx_accounts_gateway
ON accounts(gateway_id);

CREATE INDEX IF NOT EXISTS idx_accounts_expires
ON accounts(expires_at);


CREATE TABLE IF NOT EXISTS profiles (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL UNIQUE REFERENCES accounts(id) ON DELETE CASCADE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS usage (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL UNIQUE REFERENCES accounts(id) ON DELETE CASCADE,
    upload_bytes BIGINT NOT NULL DEFAULT 0 CHECK (upload_bytes >= 0),
    download_bytes BIGINT NOT NULL DEFAULT 0 CHECK (download_bytes >= 0),
    total_bytes BIGINT NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS subscriptions (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    token VARCHAR(128) NOT NULL UNIQUE,
    name VARCHAR(150) NOT NULL DEFAULT 'Nexus Subscription',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    scanner_ip TEXT,
    scanner_port INTEGER CHECK (
        scanner_port BETWEEN 1 AND 65535
    ),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_account
ON subscriptions(account_id);

CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled
ON subscriptions(enabled);

CREATE INDEX IF NOT EXISTS idx_subscriptions_token
ON subscriptions(token);


CREATE TABLE IF NOT EXISTS settings (
    id BIGSERIAL PRIMARY KEY,
    key VARCHAR(150) NOT NULL UNIQUE,
    value TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS scanner_results (
    id BIGSERIAL PRIMARY KEY,
    target TEXT NOT NULL,
    port INTEGER NOT NULL DEFAULT 443 CHECK (
        port BETWEEN 1 AND 65535
    ),
    protocol VARCHAR(30) NOT NULL DEFAULT 'tls',
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    ip TEXT,
    latency_ms INTEGER,
    websocket BOOLEAN NOT NULL DEFAULT FALSE,
    score INTEGER,
    last_checked TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scanner_results_score
ON scanner_results(score DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_scanner_results_checked
ON scanner_results(last_checked DESC);


CREATE TABLE IF NOT EXISTS gateway_logs (
    id BIGSERIAL PRIMARY KEY,
    gateway_id BIGINT
        REFERENCES gateways(id)
        ON DELETE CASCADE,
    level VARCHAR(20) NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gateway_logs_gateway
ON gateway_logs(gateway_id);

CREATE INDEX IF NOT EXISTS idx_gateway_logs_created
ON gateway_logs(created_at DESC);


INSERT INTO admin_users
(
    username,
    password_hash,
    enabled
)
VALUES
(
    'admin',
    '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918',
    TRUE
)
ON CONFLICT (username) DO NOTHING;


INSERT INTO services
(
    name,
    description,
    enabled
)
VALUES
(
    'Default',
    'Default Nexus service',
    TRUE
)
ON CONFLICT (name) DO NOTHING;


INSERT INTO settings
(
    key,
    value
)
VALUES
    ('app_name', 'Trendify Nexus'),
    ('default_region', 'eu'),
    ('gateway_path', '/ws'),
    ('subscription_path', '/sub'),
    ('version', '2.0')
ON CONFLICT (key) DO NOTHING;