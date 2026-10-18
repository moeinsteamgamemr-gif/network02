import pg from "pg";

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is missing");
}

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === "production"
    ? { rejectUnauthorized: false }
    : false,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

export async function query(text, params = []) {
  return pool.query(text, params);
}

export async function one(text, params = []) {
  const result = await pool.query(text, params);
  return result.rows[0] || null;
}

export async function many(text, params = []) {
  const result = await pool.query(text, params);
  return result.rows;
}

export async function transaction(callback) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const result = await callback({
      query: (text, params = []) =>
        client.query(text, params)
    });

    await client.query("COMMIT");

    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function initDatabase() {
  await query(`
    CREATE TABLE IF NOT EXISTS accounts (
      id BIGSERIAL PRIMARY KEY,
      username VARCHAR(100) NOT NULL,
      created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
      enabled INTEGER NOT NULL DEFAULT 1,
      display_name VARCHAR(150),
      uuid VARCHAR(100),
      note VARCHAR(500),
      updated_at BIGINT,
      data_limit BIGINT DEFAULT 0,
      data_used BIGINT DEFAULT 0,
      device_limit INTEGER DEFAULT 0,
      expires_at TEXT,
      service_id BIGINT,
      gateway_id BIGINT
    );

    CREATE TABLE IF NOT EXISTS services (
      id BIGSERIAL PRIMARY KEY,
      name VARCHAR(150) NOT NULL UNIQUE,
      description TEXT,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS gateways (
      id BIGSERIAL PRIMARY KEY,
      name VARCHAR(150) NOT NULL,
      worker_name VARCHAR(255) NOT NULL UNIQUE,
      worker_url TEXT,
      enabled INTEGER NOT NULL DEFAULT 1,
      region VARCHAR(100) DEFAULT 'default',
      status VARCHAR(50) DEFAULT 'unknown',
      latency_ms INTEGER,
      last_seen TIMESTAMPTZ,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS subscriptions (
      id BIGSERIAL PRIMARY KEY,
      account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      token VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(150),
      enabled INTEGER NOT NULL DEFAULT 1,
      scanner_ip VARCHAR(255),
      scanner_port INTEGER DEFAULT 443,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      expires_at BIGINT DEFAULT 0,
      updated_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS profiles (
      id BIGSERIAL PRIMARY KEY,
      account_id BIGINT REFERENCES accounts(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS usage (
      id BIGSERIAL PRIMARY KEY,
      account_id BIGINT REFERENCES accounts(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS settings (
      key VARCHAR(255) PRIMARY KEY,
      value TEXT
    );

    CREATE TABLE IF NOT EXISTS admin_users (
      id BIGSERIAL PRIMARY KEY,
      username VARCHAR(100) NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
      updated_at BIGINT
    );

    CREATE TABLE IF NOT EXISTS admin_sessions (
      id BIGSERIAL PRIMARY KEY,
      token_hash VARCHAR(255) NOT NULL UNIQUE,
      admin_id BIGINT NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
      expires_at BIGINT NOT NULL,
      created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT
    );

    CREATE TABLE IF NOT EXISTS scanner_results (
      id BIGSERIAL PRIMARY KEY,
      target TEXT NOT NULL,
      port INTEGER DEFAULT 443,
      protocol VARCHAR(50) DEFAULT 'tls',
      status VARCHAR(50) DEFAULT 'pending',
      latency_ms INTEGER,
      ip VARCHAR(255),
      country VARCHAR(100),
      last_checked TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      score INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS gateway_logs (
      id BIGSERIAL PRIMARY KEY,
      gateway_id BIGINT,
      event VARCHAR(255),
      detail TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await query(`
    INSERT INTO services
      (name, description, enabled)
    VALUES
      ('Default', 'Default Nexus service', 1)
    ON CONFLICT (name) DO NOTHING
  `);

  console.log("PostgreSQL database initialized");
}