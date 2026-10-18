import express from "express";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Pool } from "pg";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT || 3000);

app.set("trust proxy", 1);

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is required");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  },
  max: 5,
  min: 0,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000
});

pool.on("error", (error) => {
  console.error(
    "[PostgreSQL pool error]",
    error?.message || error
  );
});

app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

/* =========================================================
   DATABASE
========================================================= */

async function query(text, params = []) {
  return pool.query(text, params);
}

async function one(text, params = []) {
  const result = await query(text, params);
  return result.rows[0] || null;
}

async function many(text, params = []) {
  const result = await query(text, params);
  return result.rows;
}

async function transaction(callback) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function initDatabase() {
  const schemaPath = path.join(
    __dirname,
    "db",
    "schema.sql"
  );

  if (!fs.existsSync(schemaPath)) {
    throw new Error(
      `Database schema not found: ${schemaPath}`
    );
  }

  const schema = fs.readFileSync(
    schemaPath,
    "utf8"
  );

  if (!schema.trim()) {
    throw new Error(
      "Database schema is empty"
    );
  }

  /*
   * NEVER execute a destructive schema against
   * an existing Railway PostgreSQL database.
   */
  if (/\bDROP\s+TABLE\b/i.test(schema)) {
    throw new Error(
      "Refusing to execute destructive schema.sql. Remove DROP TABLE statements first."
    );
  }

  /*
   * -------------------------------------------------------
   * Run schema first.
   * CREATE TABLE IF NOT EXISTS is safe for existing DB.
   * -------------------------------------------------------
   */
  await query(schema);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
  NOT NULL DEFAULT NOW()
`);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ
  NOT NULL DEFAULT NOW()
`);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS last_error TEXT
`);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ
`);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS latency_ms INTEGER
`);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS status VARCHAR(30)
  NOT NULL DEFAULT 'creating'
`);

await query(`
  ALTER TABLE gateways
  ADD COLUMN IF NOT EXISTS region VARCHAR(30)
  NOT NULL DEFAULT 'eu'
`);

  /*
   * -------------------------------------------------------
   * LEGACY BOOLEAN MIGRATION
   *
   * Older Trendify Nexus databases used:
   *
   *   enabled INTEGER DEFAULT 1
   *
   * New schema uses:
   *
   *   enabled BOOLEAN DEFAULT TRUE
   *
   * Convert safely without deleting data.
   * -------------------------------------------------------
   */

  const booleanTables = [
    "admin_users",
    "services",
    "gateways",
    "accounts",
    "subscriptions"
  ];

  for (const table of booleanTables) {
    const columnInfo = await one(
      `
      SELECT
        data_type,
        column_default
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
        AND column_name = 'enabled'
      `,
      [table]
    );

    if (!columnInfo) {
      continue;
    }

    if (
      String(columnInfo.data_type)
        .toLowerCase() === "integer"
    ) {
      /*
       * Remove INTEGER default first.
       */
      await query(`
        ALTER TABLE ${table}
        ALTER COLUMN enabled DROP DEFAULT
      `);

      /*
       * Convert:
       * 1 -> TRUE
       * 0 -> FALSE
       */
      await query(`
        ALTER TABLE ${table}
        ALTER COLUMN enabled TYPE BOOLEAN
        USING (
          CASE
            WHEN enabled = 1 THEN TRUE
            WHEN enabled = 0 THEN FALSE
            ELSE FALSE
          END
        )
      `);
    }

    /*
     * Ensure new default is boolean.
     */
    await query(`
      ALTER TABLE ${table}
      ALTER COLUMN enabled SET DEFAULT TRUE
    `);
  }


  /*
   * -------------------------------------------------------
   * LEGACY ADMIN SESSION EXPIRATION MIGRATION
   *
   * Old version:
   *
   *   expires_at BIGINT
   *
   * New version:
   *
   *   expires_at TIMESTAMPTZ
   *
   * Old Nexus stored Unix seconds.
   * -------------------------------------------------------
   */

  const sessionColumn = await one(
    `
    SELECT
      data_type,
      column_default
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'admin_sessions'
      AND column_name = 'expires_at'
    `
  );

  if (
    sessionColumn &&
    String(sessionColumn.data_type)
      .toLowerCase() === "bigint"
  ) {
    await query(`
      ALTER TABLE admin_sessions
      ALTER COLUMN expires_at DROP DEFAULT
    `);

    await query(`
      ALTER TABLE admin_sessions
      ALTER COLUMN expires_at TYPE TIMESTAMPTZ
      USING (
        CASE
          WHEN expires_at IS NULL THEN NULL
          WHEN expires_at > 100000000000
            THEN TO_TIMESTAMP(expires_at / 1000.0)
          ELSE TO_TIMESTAMP(expires_at)
        END
      )
    `);
  }


/*
/*
 * -------------------------------------------------------
 * GATEWAYS TIMESTAMP MIGRATION
 * Legacy DB:
 *   created_at BIGINT
 *   updated_at BIGINT / missing
 *
 * New DB:
 *   created_at TIMESTAMPTZ
 *   updated_at TIMESTAMPTZ
 * -------------------------------------------------------
 */

const gatewayCreatedAt = await one(
  `
  SELECT data_type
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'gateways'
    AND column_name = 'created_at'
  `
);

if (!gatewayCreatedAt) {
  await query(`
    ALTER TABLE gateways
    ADD COLUMN created_at
    TIMESTAMPTZ
    NOT NULL
    DEFAULT CURRENT_TIMESTAMP
  `);
} else if (
  String(gatewayCreatedAt.data_type).toLowerCase() === "bigint"
) {
  await query(`
    ALTER TABLE gateways
    ALTER COLUMN created_at DROP DEFAULT
  `);

  await query(`
    ALTER TABLE gateways
    ALTER COLUMN created_at TYPE TIMESTAMPTZ
    USING (
      CASE
        WHEN created_at IS NULL
          THEN CURRENT_TIMESTAMP

        WHEN created_at > 100000000000
          THEN TO_TIMESTAMP(created_at / 1000.0)

        ELSE TO_TIMESTAMP(created_at)
      END
    )
  `);

  await query(`
    ALTER TABLE gateways
    ALTER COLUMN created_at
    SET DEFAULT CURRENT_TIMESTAMP
  `);
}


const gatewayUpdatedAt = await one(
  `
  SELECT data_type
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'gateways'
    AND column_name = 'updated_at'
  `
);

if (!gatewayUpdatedAt) {
  await query(`
    ALTER TABLE gateways
    ADD COLUMN updated_at
    TIMESTAMPTZ
    NOT NULL
    DEFAULT CURRENT_TIMESTAMP
  `);
} else if (
  String(gatewayUpdatedAt.data_type).toLowerCase() === "bigint"
) {
  await query(`
    ALTER TABLE gateways
    ALTER COLUMN updated_at DROP DEFAULT
  `);

  await query(`
    ALTER TABLE gateways
    ALTER COLUMN updated_at TYPE TIMESTAMPTZ
    USING (
      CASE
        WHEN updated_at IS NULL
          THEN CURRENT_TIMESTAMP

        WHEN updated_at > 100000000000
          THEN TO_TIMESTAMP(updated_at / 1000.0)

        ELSE TO_TIMESTAMP(updated_at)
      END
    )
  `);

  await query(`
    ALTER TABLE gateways
    ALTER COLUMN updated_at
    SET DEFAULT CURRENT_TIMESTAMP
  `);
}



  /*
   * -------------------------------------------------------
   * CLEAN EXPIRED ADMIN SESSIONS
   * -------------------------------------------------------
   */

  await query(`
    DELETE FROM admin_sessions
    WHERE expires_at <= CURRENT_TIMESTAMP
  `);


  /*
   * -------------------------------------------------------
   * DEFAULT ADMIN
   * -------------------------------------------------------
   */

  await query(`
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
    ON CONFLICT (username)
    DO NOTHING
  `);


  /*
   * -------------------------------------------------------
   * DEFAULT SERVICE
   * -------------------------------------------------------
   */

  await query(`
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
    ON CONFLICT (name)
    DO NOTHING
  `);


  /*
   * -------------------------------------------------------
   * DEFAULT SETTINGS
   * -------------------------------------------------------
   */

  await query(`
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
    ON CONFLICT (key)
    DO NOTHING
  `);


  console.log(
    "Trendify Nexus database initialized"
  );
}
/* =========================================================
   HELPERS
========================================================= */

function sha256(value) {
  return crypto
    .createHash("sha256")
    .update(String(value))
    .digest("hex");
}

function randomString(length = 32) {
  const chars =
    "abcdefghijklmnopqrstuvwxyz0123456789";

  let result = "";

  while (result.length < length) {
    const bytes = crypto.randomBytes(length);

    for (const byte of bytes) {
      result += chars[byte % chars.length];

      if (result.length >= length) {
        break;
      }
    }
  }

  return result;
}

function generateUUID() {
  return crypto.randomUUID();
}

function json(res, data, status = 200) {
  return res
    .status(status)
    .json(data);
}

function toNumber(value, fallback = 0) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;
}

function getCookie(req, name) {
  const header = req.headers.cookie;

  if (!header) {
    return null;
  }

  const cookies = header
    .split(";")
    .map(x => x.trim());

  for (const cookie of cookies) {
    const index = cookie.indexOf("=");

    if (index === -1) {
      continue;
    }

    const key = cookie
      .slice(0, index)
      .trim();

    if (key !== name) {
      continue;
    }

    return decodeURIComponent(
      cookie.slice(index + 1)
    );
  }

  return null;
}



function isHttpsRequest(req) {
  if (!req || typeof req !== "object") {
    return false;
  }

  if (req.secure) {
    return true;
  }

  const forwarded = String(
    req.headers?.["x-forwarded-proto"] || ""
  )
    .split(",")[0]
    .trim()
    .toLowerCase();

  return forwarded === "https";
}


function setSessionCookie(
  req,
  res,
  token
) {
  const parts = [
    `nexus_session=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    "Max-Age=604800"
  ];

  if (isHttpsRequest(req)) {
    parts.push("Secure");
  }

  res.setHeader(
    "Set-Cookie",
    parts.join("; ")
  );
}

function clearSessionCookie(res) {
  res.setHeader(
    "Set-Cookie",
    [
      "nexus_session=",
      "Path=/",
      "HttpOnly",
      "SameSite=Lax",
      "Max-Age=0"
    ].join("; ")
  );
}

function publicBaseUrl(req) {
  const configured =
    String(
      process.env.PUBLIC_URL || ""
    )
      .trim()
      .replace(/\/+$/, "");

  if (configured) {
    return configured;
  }

  const protocol =
    isHttpsRequest(req)
      ? "https"
      : "http";

  const host =
    req.get("host");

  return `${protocol}://${host}`;
}

function normalizeWorkerUrl(value) {
  const input =
    String(value || "").trim();

  if (!input) {
    return "";
  }

  try {
    const url = new URL(
      /^https?:\/\//i.test(input)
        ? input
        : `https://${input}`
    );

    return url.host;
  } catch {
    return input
      .replace(
        /^https?:\/\//i,
        ""
      )
      .split("/")[0]
      .trim();
  }
}

function normalizeDate(value) {
  if (
    value === null ||
    value === undefined ||
    value === ""
  ) {
    return null;
  }

  const date =
    value instanceof Date
      ? value
      : new Date(value);

  if (
    Number.isNaN(
      date.getTime()
    )
  ) {
    return null;
  }

  return date;
}

/* =========================================================
   ADMIN AUTH
========================================================= */

async function getAdminFromRequest(req) {
  const token =
    getCookie(
      req,
      "nexus_session"
    );

  if (!token) {
    return null;
  }

  const tokenHash =
    sha256(token);

  return one(
    `
    SELECT
      au.id,
      au.username,
      au.enabled
    FROM admin_sessions s
    JOIN admin_users au
      ON au.id = s.admin_id
    WHERE
      s.token_hash = $1
      AND s.expires_at > CURRENT_TIMESTAMP
      AND au.enabled = TRUE
    LIMIT 1
    `,
    [tokenHash]
  );
}

async function requireAdmin(req, res) {
  const admin =
    await getAdminFromRequest(req);

  if (!admin) {
    json(
      res,
      {
        success: false,
        error: "Unauthorized"
      },
      401
    );

    return false;
  }

  req.admin = admin;

  return true;
}

/* =========================================================
   AUTH STATUS
========================================================= */

app.get(
  "/api/auth/setup-status",
  async (req, res) => {
    try {
      const row =
        await one(`
          SELECT
            COUNT(*)::integer AS count
          FROM admin_users
          WHERE enabled = TRUE
        `);

      return json(res, {
        success: true,
        initialized:
          Number(row?.count || 0) > 0,
        setup_required:
          Number(row?.count || 0) === 0
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   LOGIN
========================================================= */

app.post(
  "/api/auth/login",
  async (req, res) => {
    try {
      const username =
        String(
          req.body?.username || ""
        ).trim();

      const password =
        String(
          req.body?.password || ""
        );

      if (!username || !password) {
        return json(
          res,
          {
            success: false,
            error:
              "Username and password are required"
          },
          400
        );
      }

      const admin =
        await one(
          `
          SELECT
            id,
            username,
            password_hash,
            enabled
          FROM admin_users
          WHERE username = $1
          LIMIT 1
          `,
          [username]
        );

      if (
        !admin ||
        !admin.enabled ||
        !crypto.timingSafeEqual(
          Buffer.from(
            sha256(password),
            "utf8"
          ),
          Buffer.from(
            String(admin.password_hash),
            "utf8"
          )
        )
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Invalid username or password"
          },
          401
        );
      }

      const sessionToken =
        crypto.randomBytes(48)
          .toString("hex");

      const expiresAt =
        new Date(
          Date.now() +
            7 * 24 * 60 * 60 * 1000
        );

      await query(
        `
        INSERT INTO admin_sessions
        (
          admin_id,
          token_hash,
          expires_at,
          created_at
        )
        VALUES
        (
          $1,
          $2,
          $3,
          CURRENT_TIMESTAMP
        )
        `,
        [
          admin.id,
          sha256(sessionToken),
          expiresAt
        ]
      );

      setSessionCookie(
        req,
        res,
        sessionToken
      );

      return json(res, {
        success: true,
        admin: {
          id: Number(admin.id),
          username: admin.username
        }
      });
    } catch (error) {
      console.error(
        "Login error:",
        error
      );

      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   LOGOUT
========================================================= */

app.post(
  "/api/auth/logout",
  async (req, res) => {
    try {
      const token =
        getCookie(
          req,
          "nexus_session"
        );

      if (token) {
        await query(
          `
          DELETE FROM admin_sessions
          WHERE token_hash = $1
          `,
          [sha256(token)]
        );
      }

      clearSessionCookie(res);

      return json(res, {
        success: true
      });
    } catch (error) {
      clearSessionCookie(res);

      return json(res, {
        success: true
      });
    }
  }
);

/* =========================================================
   CURRENT ADMIN
========================================================= */

app.get(
  "/api/auth/me",
  async (req, res) => {
    try {
      const admin =
        await getAdminFromRequest(req);

      if (!admin) {
        return json(
          res,
          {
            success: false,
            authenticated: false
          },
          401
        );
      }

      return json(res, {
        success: true,
        authenticated: true,
        admin: {
          id: Number(admin.id),
          username: admin.username
        }
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   TEST AUTH
========================================================= */

app.get(
  "/api/auth/test",
  async (req, res) => {
    if (!(await requireAdmin(req, res))) {
      return;
    }

    return json(res, {
      success: true,
      authenticated: true,
      admin: {
        id: Number(req.admin.id),
        username: req.admin.username
      }
    });
  }
);

/* =========================================================
   CHANGE PASSWORD
========================================================= */

app.post(
  "/api/auth/change-password",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const currentPassword =
        String(
          req.body?.currentPassword ||
            req.body?.current_password ||
            ""
        );

      const newPassword =
        String(
          req.body?.newPassword ||
            req.body?.new_password ||
            ""
        );

      if (
        !currentPassword ||
        !newPassword
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Current and new password are required"
          },
          400
        );
      }

      if (newPassword.length < 8) {
        return json(
          res,
          {
            success: false,
            error:
              "New password must be at least 8 characters"
          },
          400
        );
      }

      const admin =
        await one(
          `
          SELECT
            id,
            password_hash
          FROM admin_users
          WHERE id = $1
          `,
          [req.admin.id]
        );

      if (!admin) {
        return json(
          res,
          {
            success: false,
            error: "Admin not found"
          },
          404
        );
      }

      if (
        sha256(currentPassword) !==
        String(admin.password_hash)
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Current password is incorrect"
          },
          401
        );
      }

      await query(
        `
        UPDATE admin_users
        SET
          password_hash = $1,
          updated_at = CURRENT_TIMESTAMP
        WHERE id = $2
        `,
        [
          sha256(newPassword),
          admin.id
        ]
      );

      return json(res, {
        success: true
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   HEALTH
========================================================= */

app.get(
  "/api/health",
  async (req, res) => {
    try {
      await query("SELECT 1");

      return json(res, {
        success: true,
        status: "online",
        app: "Trendify Nexus",
        version: "2.0",
        timestamp:
          new Date().toISOString()
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          status: "error",
          error: error.message
        },
        503
      );
    }
  }
);

/* =========================================================
   DASHBOARD STATS
========================================================= */

app.get(
  "/api/stats",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const stats =
        await one(`
          SELECT
            (
              SELECT COUNT(*)
              FROM accounts
            )::integer AS users,

            (
              SELECT COUNT(*)
              FROM accounts
              WHERE enabled = TRUE
            )::integer AS active_users,

            (
              SELECT COUNT(*)
              FROM gateways
            )::integer AS gateways,

            (
              SELECT COUNT(*)
              FROM gateways
              WHERE enabled = TRUE
              AND status = 'online'
            )::integer AS online_gateways,

            (
              SELECT COUNT(*)
              FROM subscriptions
            )::integer AS subscriptions,

            (
              SELECT COALESCE(
                SUM(data_used),
                0
              )
              FROM accounts
            )::bigint AS data_used
        `);

      return json(res, {
        success: true,
        stats: {
          users: Number(stats.users || 0),
          active_users:
            Number(
              stats.active_users || 0
            ),
          gateways:
            Number(stats.gateways || 0),
          online_gateways:
            Number(
              stats.online_gateways || 0
            ),
          subscriptions:
            Number(
              stats.subscriptions || 0
            ),
          data_used:
            Number(
              stats.data_used || 0
            )
        }
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   SERVICES
========================================================= */

app.get(
  "/api/services",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const services =
        await many(`
          SELECT *
          FROM services
          ORDER BY id ASC
        `);

      return json(res, {
        success: true,
        services
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

app.post(
  "/api/services",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const name =
        String(
          req.body?.name || ""
        )
          .trim()
          .slice(0, 100);

      const description =
        String(
          req.body?.description || ""
        ).slice(0, 2000);

      if (!name) {
        return json(
          res,
          {
            success: false,
            error: "Service name is required"
          },
          400
        );
      }

      const service =
        await one(
          `
          INSERT INTO services
          (
            name,
            description,
            enabled,
            created_at,
            updated_at
          )
          VALUES
          (
            $1,
            $2,
            TRUE,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
          )
          RETURNING *
          `,
          [
            name,
            description
          ]
        );

      return json(
        res,
        {
          success: true,
          service
        },
        201
      );
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        400
      );
    }
  }
);

app.patch(
  "/api/services/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      if (!Number.isInteger(id)) {
        return json(
          res,
          {
            success: false,
            error: "Invalid service id"
          },
          400
        );
      }

      const service =
        await one(
          `
          UPDATE services
          SET
            enabled = NOT enabled,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $1
          RETURNING *
          `,
          [id]
        );

      if (!service) {
        return json(
          res,
          {
            success: false,
            error: "Service not found"
          },
          404
        );
      }

      return json(res, {
        success: true,
        service
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   ACCOUNTS - LIST
========================================================= */

app.get(
  "/api/accounts",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const accounts =
        await many(`
          SELECT
            a.*,

            p.metadata AS profile_metadata,

            s.name AS service_name,

            g.name AS gateway_name,
            g.worker_name AS gateway_worker_name,
            g.worker_url AS gateway_url,
            g.status AS gateway_status,
            g.region AS gateway_region,

            sub.id AS subscription_id,
            sub.token AS subscription_token,
            sub.name AS subscription_name,
            sub.enabled AS subscription_enabled,
            sub.expires_at AS subscription_expires_at,

            u.upload_bytes,
            u.download_bytes,
            u.total_bytes

          FROM accounts a

          LEFT JOIN profiles p
            ON p.account_id = a.id

          LEFT JOIN services s
            ON s.id = a.service_id

          LEFT JOIN gateways g
            ON g.id = a.gateway_id

          LEFT JOIN LATERAL (
            SELECT *
            FROM subscriptions
            WHERE account_id = a.id
            ORDER BY id ASC
            LIMIT 1
          ) sub
            ON TRUE

          LEFT JOIN usage u
            ON u.account_id = a.id

          ORDER BY a.id DESC
        `);

      return json(res, {
        success: true,
        accounts
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   ACCOUNT CREATE
========================================================= */

app.post(
  "/api/accounts",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const username =
        String(
          req.body?.username || ""
        )
          .trim()
          .slice(0, 100);

      const displayName =
        String(
          req.body?.display_name ??
          req.body?.displayName ??
          username
        )
          .trim()
          .slice(0, 150);

      const note =
        String(
          req.body?.note || ""
        ).slice(0, 5000);

      const dataLimit =
        Math.max(
          0,
          Math.floor(
            toNumber(
              req.body?.data_limit ??
              req.body?.dataLimit,
              0
            )
          )
        );

      const deviceLimit =
        Math.max(
          0,
          Math.floor(
            toNumber(
              req.body?.device_limit ??
              req.body?.deviceLimit,
              0
            )
          )
        );

      const serviceId =
        req.body?.service_id ??
        req.body?.serviceId ??
        null;

      const gatewayId =
        req.body?.gateway_id ??
        req.body?.gatewayId ??
        null;

      const expiresAt =
        normalizeDate(
          req.body?.expires_at ??
          req.body?.expiresAt
        );

      if (!username) {
        return json(
          res,
          {
            success: false,
            error: "Username is required"
          },
          400
        );
      }

      if (
        expiresAt &&
        expiresAt.getTime() <= Date.now()
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Expiration date must be in the future"
          },
          400
        );
      }

      let service = null;
      let gateway = null;

      if (serviceId !== null) {
        service =
          await one(
            `
            SELECT *
            FROM services
            WHERE id = $1
            AND enabled = TRUE
            `,
            [Number(serviceId)]
          );

        if (!service) {
          return json(
            res,
            {
              success: false,
              error:
                "Selected service is invalid or disabled"
            },
            400
          );
        }
      }

      if (gatewayId !== null) {
        gateway =
          await one(
            `
            SELECT *
            FROM gateways
            WHERE id = $1
            AND enabled = TRUE
            AND status = 'online'
            `,
            [Number(gatewayId)]
          );

        if (!gateway) {
          return json(
            res,
            {
              success: false,
              error:
                "Selected gateway is invalid or offline"
            },
            400
          );
        }
      }

      const result =
        await transaction(
          async client => {
            const accountResult =
              await client.query(
                `
                INSERT INTO accounts
                (
                  username,
                  display_name,
                  uuid,
                  enabled,
                  note,
                  data_limit,
                  data_used,
                  device_limit,
                  expires_at,
                  service_id,
                  gateway_id,
                  created_at,
                  updated_at
                )
                VALUES
                (
                  $1,
                  $2,
                  $3,
                  TRUE,
                  $4,
                  $5,
                  0,
                  $6,
                  $7,
                  $8,
                  $9,
                  CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP
                )
                RETURNING *
                `,
                [
                  username,
                  displayName,
                  generateUUID(),
                  note,
                  dataLimit,
                  deviceLimit,
                  expiresAt,
                  serviceId === null
                    ? null
                    : Number(serviceId),
                  gatewayId === null
                    ? null
                    : Number(gatewayId)
                ]
              );

            const account =
              accountResult.rows[0];

            await client.query(
              `
              INSERT INTO profiles
              (
                account_id,
                metadata,
                created_at,
                updated_at
              )
              VALUES
              (
                $1,
                '{}'::jsonb,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
              )
              `,
              [account.id]
            );

            await client.query(
              `
              INSERT INTO usage
              (
                account_id,
                upload_bytes,
                download_bytes,
                total_bytes,
                updated_at
              )
              VALUES
              (
                $1,
                0,
                0,
                0,
                CURRENT_TIMESTAMP
              )
              `,
              [account.id]
            );

            const token =
              randomString(64);

            const subscriptionResult =
              await client.query(
                `
                INSERT INTO subscriptions
                (
                  account_id,
                  token,
                  name,
                  enabled,
                  scanner_ip,
                  scanner_port,
                  expires_at,
                  created_at,
                  updated_at
                )
                VALUES
                (
                  $1,
                  $2,
                  $3,
                  TRUE,
                  NULL,
                  443,
                  $4,
                  CURRENT_TIMESTAMP,
                  CURRENT_TIMESTAMP
                )
                RETURNING *
                `,
                [
                  account.id,
                  token,
                  `${displayName || username} Subscription`,
                  expiresAt
                ]
              );

            return {
              account,
              subscription:
                subscriptionResult.rows[0]
            };
          }
        );

      const base =
        publicBaseUrl(req);

      return json(
        res,
        {
          success: true,
          account: result.account,
          subscription:
            result.subscription,
          subscription_url:
            `${base}/sub/${result.subscription.token}`
        },
        201
      );
    } catch (error) {
      console.error(
        "Account create error:",
        error
      );

      return json(
        res,
        {
          success: false,
          error: error.message
        },
        400
      );
    }
  }
);

/* =========================================================
   ACCOUNT UPDATE
========================================================= */

app.put(
  "/api/accounts/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      if (!Number.isInteger(id)) {
        return json(
          res,
          {
            success: false,
            error: "Invalid account id"
          },
          400
        );
      }

      const existing =
        await one(
          `
          SELECT *
          FROM accounts
          WHERE id = $1
          `,
          [id]
        );

      if (!existing) {
        return json(
          res,
          {
            success: false,
            error: "Account not found"
          },
          404
        );
      }

      const username =
        String(
          req.body?.username ??
          existing.username
        )
          .trim()
          .slice(0, 100);

      const displayName =
        String(
          req.body?.display_name ??
          req.body?.displayName ??
          existing.display_name
        )
          .trim()
          .slice(0, 150);

      const note =
        String(
          req.body?.note ??
          existing.note ??
          ""
        ).slice(0, 5000);

      const dataLimit =
        Math.max(
          0,
          Math.floor(
            toNumber(
              req.body?.data_limit ??
              req.body?.dataLimit,
              Number(existing.data_limit || 0)
            )
          )
        );

      const deviceLimit =
        Math.max(
          0,
          Math.floor(
            toNumber(
              req.body?.device_limit ??
              req.body?.deviceLimit,
              Number(existing.device_limit || 0)
            )
          )
        );

      const expiresAt =
        req.body &&
        (
          "expires_at" in req.body ||
          "expiresAt" in req.body
        )
          ? normalizeDate(
              req.body.expires_at ??
              req.body.expiresAt
            )
          : existing.expires_at;

      const serviceId =
        req.body &&
        (
          "service_id" in req.body ||
          "serviceId" in req.body
        )
          ? (
              req.body.service_id ??
              req.body.serviceId
            )
          : existing.service_id;

      const gatewayId =
        req.body &&
        (
          "gateway_id" in req.body ||
          "gatewayId" in req.body
        )
          ? (
              req.body.gateway_id ??
              req.body.gatewayId
            )
          : existing.gateway_id;

      if (!username) {
        return json(
          res,
          {
            success: false,
            error: "Username is required"
          },
          400
        );
      }

      if (
        expiresAt &&
        new Date(expiresAt).getTime() <=
          Date.now()
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Expiration date must be in the future"
          },
          400
        );
      }

      if (serviceId !== null) {
        const service =
          await one(
            `
            SELECT id
            FROM services
            WHERE id = $1
            AND enabled = TRUE
            `,
            [Number(serviceId)]
          );

        if (!service) {
          return json(
            res,
            {
              success: false,
              error:
                "Selected service is invalid or disabled"
            },
            400
          );
        }
      }

      if (gatewayId !== null) {
        const gateway =
          await one(
            `
            SELECT id
            FROM gateways
            WHERE id = $1
            AND enabled = TRUE
            AND status = 'online'
            `,
            [Number(gatewayId)]
          );

        if (!gateway) {
          return json(
            res,
            {
              success: false,
              error:
                "Selected gateway is invalid or offline"
            },
            400
          );
        }
      }

      const account =
        await one(
          `
          UPDATE accounts
          SET
            username = $1,
            display_name = $2,
            note = $3,
            data_limit = $4,
            device_limit = $5,
            expires_at = $6,
            service_id = $7,
            gateway_id = $8,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $9
          RETURNING *
          `,
          [
            username,
            displayName,
            note,
            dataLimit,
            deviceLimit,
            expiresAt,
            serviceId === null
              ? null
              : Number(serviceId),
            gatewayId === null
              ? null
              : Number(gatewayId),
            id
          ]
        );

      return json(res, {
        success: true,
        account
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        400
      );
    }
  }
);

/* =========================================================
   ACCOUNT TOGGLE
========================================================= */

app.patch(
  "/api/accounts/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const account =
        await one(
          `
          UPDATE accounts
          SET
            enabled = NOT enabled,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $1
          RETURNING *
          `,
          [id]
        );

      if (!account) {
        return json(
          res,
          {
            success: false,
            error: "Account not found"
          },
          404
        );
      }

      return json(res, {
        success: true,
        account
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   ACCOUNT RESET USAGE
========================================================= */

app.post(
  "/api/accounts/:id/reset",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const account =
        await one(
          `
          UPDATE accounts
          SET
            data_used = 0,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $1
          RETURNING *
          `,
          [id]
        );

      if (!account) {
        return json(
          res,
          {
            success: false,
            error: "Account not found"
          },
          404
        );
      }

      await query(
        `
        UPDATE usage
        SET
          upload_bytes = 0,
          download_bytes = 0,
          total_bytes = 0,
          updated_at = CURRENT_TIMESTAMP
        WHERE account_id = $1
        `,
        [id]
      );

      return json(res, {
        success: true,
        account
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   ACCOUNT DELETE
========================================================= */

app.delete(
  "/api/accounts/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const account =
        await one(
          `
          SELECT id
          FROM accounts
          WHERE id = $1
          `,
          [id]
        );

      if (!account) {
        return json(
          res,
          {
            success: false,
            error: "Account not found"
          },
          404
        );
      }

      /*
       * Profiles, usage and subscriptions
       * already have ON DELETE CASCADE.
       */
      await query(
        `
        DELETE FROM accounts
        WHERE id = $1
        `,
        [id]
      );

      return json(res, {
        success: true
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   SUBSCRIPTIONS - LIST
========================================================= */

app.get(
  "/api/subscriptions",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const subscriptions =
        await many(`
          SELECT
            s.*,

            a.username AS account_username,
            a.display_name AS account_display_name,
            a.uuid AS account_uuid,
            a.enabled AS account_enabled,
            a.gateway_id AS account_gateway_id,

            g.name AS gateway_name,
            g.worker_url AS gateway_url,
            g.status AS gateway_status,
            g.region AS gateway_region

          FROM subscriptions s

          JOIN accounts a
            ON a.id = s.account_id

          LEFT JOIN gateways g
            ON g.id = a.gateway_id

          ORDER BY s.id DESC
        `);

      const base =
        publicBaseUrl(req);

      return json(res, {
        success: true,

        subscriptions:
          subscriptions.map(item => ({
            ...item,
            subscription_url:
              `${base}/sub/${item.token}`
          }))
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   SUBSCRIPTION - CREATE
========================================================= */

app.post(
  "/api/subscriptions",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const accountId =
        Number(
          req.body?.account_id ??
          req.body?.accountId
        );

      const name =
        String(
          req.body?.name ||
            "Nexus Subscription"
        )
          .trim()
          .slice(0, 150);

      const expiresAt =
        normalizeDate(
          req.body?.expires_at ??
          req.body?.expiresAt
        );

      if (
        !Number.isInteger(accountId)
      ) {
        return json(
          res,
          {
            success: false,
            error: "Invalid account id"
          },
          400
        );
      }

      if (
        expiresAt &&
        expiresAt.getTime() <= Date.now()
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Expiration date must be in the future"
          },
          400
        );
      }

      const account =
        await one(
          `
          SELECT
            id,
            username,
            enabled
          FROM accounts
          WHERE id = $1
          `,
          [accountId]
        );

      if (!account) {
        return json(
          res,
          {
            success: false,
            error: "Account not found"
          },
          404
        );
      }

      const token =
        randomString(64);

      const subscription =
        await one(
          `
          INSERT INTO subscriptions
          (
            account_id,
            token,
            name,
            enabled,
            scanner_ip,
            scanner_port,
            expires_at,
            created_at,
            updated_at
          )
          VALUES
          (
            $1,
            $2,
            $3,
            TRUE,
            NULL,
            443,
            $4,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
          )
          RETURNING *
          `,
          [
            accountId,
            token,
            name ||
              "Nexus Subscription",
            expiresAt
          ]
        );

      const base =
        publicBaseUrl(req);

      return json(
        res,
        {
          success: true,
          subscription,
          subscription_url:
            `${base}/sub/${token}`
        },
        201
      );
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        400
      );
    }
  }
);

/* =========================================================
   SUBSCRIPTION - UPDATE
========================================================= */

app.put(
  "/api/subscriptions/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      if (!Number.isInteger(id)) {
        return json(
          res,
          {
            success: false,
            error:
              "Invalid subscription id"
          },
          400
        );
      }

      const existing =
        await one(
          `
          SELECT *
          FROM subscriptions
          WHERE id = $1
          `,
          [id]
        );

      if (!existing) {
        return json(
          res,
          {
            success: false,
            error:
              "Subscription not found"
          },
          404
        );
      }

      const name =
        String(
          req.body?.name ??
          existing.name
        )
          .trim()
          .slice(0, 150);

      const expiresAt =
        (
          req.body &&
          (
            "expires_at" in req.body ||
            "expiresAt" in req.body
          )
        )
          ? normalizeDate(
              req.body.expires_at ??
              req.body.expiresAt
            )
          : existing.expires_at;

      if (
        expiresAt &&
        expiresAt.getTime() <= Date.now()
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Expiration date must be in the future"
          },
          400
        );
      }

      const subscription =
        await one(
          `
          UPDATE subscriptions
          SET
            name = $1,
            expires_at = $2,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $3
          RETURNING *
          `,
          [
            name ||
              "Nexus Subscription",
            expiresAt,
            id
          ]
        );

      return json(res, {
        success: true,
        subscription
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        400
      );
    }
  }
);

/* =========================================================
   SUBSCRIPTION - TOGGLE
========================================================= */

app.patch(
  "/api/subscriptions/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const subscription =
        await one(
          `
          UPDATE subscriptions
          SET
            enabled = NOT enabled,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $1
          RETURNING *
          `,
          [id]
        );

      if (!subscription) {
        return json(
          res,
          {
            success: false,
            error:
              "Subscription not found"
          },
          404
        );
      }

      return json(res, {
        success: true,
        subscription
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   SUBSCRIPTION - DELETE
========================================================= */

app.delete(
  "/api/subscriptions/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const deleted =
        await one(
          `
          DELETE FROM subscriptions
          WHERE id = $1
          RETURNING id
          `,
          [id]
        );

      if (!deleted) {
        return json(
          res,
          {
            success: false,
            error:
              "Subscription not found"
          },
          404
        );
      }

      return json(res, {
        success: true
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   SUBSCRIPTION - ADMIN DETAILS
========================================================= */

app.get(
  "/api/subscriptions/:token",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const token =
        String(
          req.params.token || ""
        ).trim();

      if (!token) {
        return json(
          res,
          {
            success: false,
            error:
              "Subscription token is required"
          },
          400
        );
      }

      const subscription =
        await one(
          `
          SELECT
            s.*,

            a.username,
            a.display_name,
            a.uuid,
            a.enabled AS account_enabled,
            a.data_limit,
            a.data_used,
            a.device_limit,
            a.expires_at AS account_expires_at,
            a.gateway_id,

            g.name AS gateway_name,
            g.worker_name AS gateway_worker_name,
            g.worker_url AS gateway_url,
            g.status AS gateway_status,
            g.region AS gateway_region

          FROM subscriptions s

          JOIN accounts a
            ON a.id = s.account_id

          LEFT JOIN gateways g
            ON g.id = a.gateway_id

          WHERE s.token = $1

          LIMIT 1
          `,
          [token]
        );

      if (!subscription) {
        return json(
          res,
          {
            success: false,
            error:
              "Subscription not found"
          },
          404
        );
      }

      return json(res, {
        success: true,
        subscription
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   PUBLIC SUBSCRIPTION
========================================================= */

app.get(
  "/sub/:token",
  async (req, res) => {
    try {
      const token =
        String(
          req.params.token || ""
        ).trim();

      if (!token) {
        return new Response(
          "Subscription token is required",
          {
            status: 400
          }
        );
      }

      const subscription =
        await one(
          `
          SELECT
            s.id,
            s.token,
            s.name AS subscription_name,
            s.enabled AS subscription_enabled,
            s.expires_at AS subscription_expires_at,

            a.id AS account_id,
            a.username,
            a.display_name,
            a.uuid,
            a.enabled AS account_enabled,
            a.expires_at AS account_expires_at,
            a.data_limit,
            a.data_used,
            a.gateway_id,

            g.worker_url,
            g.status AS gateway_status,
            g.enabled AS gateway_enabled,
            g.region AS gateway_region

          FROM subscriptions s

          JOIN accounts a
            ON a.id = s.account_id

          LEFT JOIN gateways g
            ON g.id = a.gateway_id

          WHERE s.token = $1
          LIMIT 1
          `,
          [token]
        );

      const headers = {
        "content-type":
          "text/plain; charset=utf-8",

        "access-control-allow-origin":
          "*",

        "cache-control":
          "no-store, no-cache, must-revalidate",

        "pragma":
          "no-cache"
      };

      if (!subscription) {
        return res
          .status(404)
          .set(headers)
          .send(
            "Subscription not found"
          );
      }

      if (
        !subscription.subscription_enabled ||
        !subscription.account_enabled
      ) {
        return res
          .status(403)
          .set(headers)
          .send(
            "Subscription is disabled"
          );
      }

      const now =
        Date.now();

      if (
        subscription.subscription_expires_at &&
        new Date(
          subscription.subscription_expires_at
        ).getTime() <= now
      ) {
        return res
          .status(403)
          .set(headers)
          .send(
            "Subscription has expired"
          );
      }

      if (
        subscription.account_expires_at &&
        new Date(
          subscription.account_expires_at
        ).getTime() <= now
      ) {
        return res
          .status(403)
          .set(headers)
          .send(
            "Account has expired"
          );
      }

      if (
        Number(subscription.data_limit || 0) > 0 &&
        Number(subscription.data_used || 0) >=
          Number(subscription.data_limit)
      ) {
        return res
          .status(403)
          .set(headers)
          .send(
            "Data limit reached"
          );
      }

      if (
        !subscription.gateway_id ||
        !subscription.worker_url ||
        !subscription.gateway_enabled ||
        subscription.gateway_status !==
          "online"
      ) {
        return res
          .status(503)
          .set(headers)
          .send(
            "Assigned gateway is not available"
          );
      }

      /*
       * IMPORTANT:
       *
       * scanner_ip is deliberately NOT used.
       *
       * The VLESS server is always the
       * Trendify Nexus Gateway Worker.
       */
      const gatewayHost =
        normalizeWorkerUrl(
          subscription.worker_url
        );

      if (!gatewayHost) {
        return res
          .status(503)
          .set(headers)
          .send(
            "Gateway URL is invalid"
          );
      }

      const pathValue =
        "/ws";

      const vless =
        `vless://${subscription.uuid}` +
        `@${gatewayHost}:443` +
        `?encryption=none` +
        `&security=tls` +
        `&sni=${encodeURIComponent(
          gatewayHost
        )}` +
        `&fp=chrome` +
        `&type=ws` +
        `&host=${encodeURIComponent(
          gatewayHost
        )}` +
        `&path=${encodeURIComponent(
          pathValue
        )}` +
        `#${encodeURIComponent(
          subscription.subscription_name ||
          subscription.display_name ||
          subscription.username
        )}`;

      return res
        .status(200)
        .set(headers)
        .send(vless);
    } catch (error) {
      console.error(
        "Public subscription error:",
        error
      );

      return res
        .status(500)
        .set({
          "content-type":
            "text/plain; charset=utf-8",
          "access-control-allow-origin":
            "*"
        })
        .send(
          "Subscription error"
        );
    }
  }
);

/* =========================================================
   GATEWAYS - LIST
========================================================= */

app.get(
  "/api/gateways",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const gateways =
        await many(`
          SELECT
            *
          FROM gateways
          ORDER BY id DESC
        `);

      return json(res, {
        success: true,
        gateways
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   GATEWAY INFO
========================================================= */

app.get(
  "/api/gateways/:id/info",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const gateway =
        await one(
          `
          SELECT *
          FROM gateways
          WHERE id = $1
          `,
          [id]
        );

      if (!gateway) {
        return json(
          res,
          {
            success: false,
            error: "Gateway not found"
          },
          404
        );
      }

      let health = null;
      let healthError = null;

      if (gateway.worker_url) {
        try {
          health =
            await testGatewayHealth(
              gateway.worker_url
            );
        } catch (error) {
          healthError =
            error?.message ||
            String(error);
        }
      }

      return json(res, {
        success: true,
        gateway,
        health,
        health_error:
          healthError
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   GATEWAY TOGGLE
========================================================= */

app.patch(
  "/api/gateways/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const existing =
        await one(
          `
          SELECT *
          FROM gateways
          WHERE id = $1
          `,
          [id]
        );

      if (!existing) {
        return json(
          res,
          {
            success: false,
            error: "Gateway not found"
          },
          404
        );
      }

      const nextEnabled =
        !Boolean(existing.enabled);

      const gateway =
        await one(
          `
          UPDATE gateways
          SET
            enabled = $1,
            status =
              CASE
                WHEN $1 = FALSE
                  THEN 'disabled'
                WHEN worker_url IS NULL
                  THEN 'creating'
                ELSE status
              END,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $2
          RETURNING *
          `,
          [
            nextEnabled,
            id
          ]
        );

      return json(res, {
        success: true,
        gateway
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   GATEWAY DELETE
========================================================= */

app.delete(
  "/api/gateways/:id",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      const id =
        Number(req.params.id);

      const gateway =
        await one(
          `
          SELECT id
          FROM gateways
          WHERE id = $1
          `,
          [id]
        );

      if (!gateway) {
        return json(
          res,
          {
            success: false,
            error: "Gateway not found"
          },
          404
        );
      }

      const assigned =
        await one(
          `
          SELECT COUNT(*)::integer AS count
          FROM accounts
          WHERE gateway_id = $1
          `,
          [id]
        );

      if (
        Number(assigned?.count || 0) > 0
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Gateway is assigned to one or more accounts"
          },
          409
        );
      }

      await query(
        `
        DELETE FROM gateways
        WHERE id = $1
        `,
        [id]
      );

      return json(res, {
        success: true
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);


/* =========================================================
   TELEGRAM BOT API â€” READ ONLY
========================================================= */

function botSecretValid(req) {
  const configured = String(
    process.env.NEXUS_BOT_SECRET || ""
  ).trim();

  const supplied = String(
    req.headers["x-nexus-bot-secret"] || ""
  ).trim();

  if (!configured || !supplied) return false;

  const a = Buffer.from(configured, "utf8");
  const b = Buffer.from(supplied, "utf8");

  if (a.length !== b.length) return false;

  return crypto.timingSafeEqual(a, b);
}

function requireBotSecret(req, res) {
  if (botSecretValid(req)) return true;

  json(res, {
    success: false,
    error: "Unauthorized bot"
  }, 401);

  return false;
}

app.get("/api/internal/bot/stats", async (req, res) => {
  try {
    if (!requireBotSecret(req, res)) return;

    const stats = await one(`
      SELECT
        (SELECT COUNT(*) FROM accounts)::integer AS users,
        (SELECT COUNT(*) FROM accounts
          WHERE enabled = TRUE)::integer AS active_users,
        (SELECT COUNT(*) FROM gateways)::integer AS gateways,
        (SELECT COUNT(*) FROM gateways
          WHERE enabled = TRUE
          AND status = 'online')::integer AS online_gateways,
        (SELECT COUNT(*) FROM subscriptions
          WHERE enabled = TRUE)::integer AS subscriptions
    `);

    return json(res, {
      success: true,
      stats: {
        users: Number(stats.users || 0),
        active_users: Number(stats.active_users || 0),
        gateways: Number(stats.gateways || 0),
        online_gateways: Number(stats.online_gateways || 0),
        subscriptions: Number(stats.subscriptions || 0)
      }
    });
  } catch (error) {
    return json(res, {
      success: false,
      error: "Could not load statistics"
    }, 500);
  }
});

app.get("/api/internal/bot/users", async (req, res) => {
  try {
    if (!requireBotSecret(req, res)) return;

    const users = await many(`
      SELECT id, username, display_name, enabled,
             data_limit, data_used, expires_at
      FROM accounts
      ORDER BY id DESC
      LIMIT 10
    `);

    return json(res, { success: true, users });
  } catch (error) {
    return json(res, {
      success: false,
      error: "Could not load users"
    }, 500);
  }
});

app.get("/api/internal/bot/gateways", async (req, res) => {
  try {
    if (!requireBotSecret(req, res)) return;

    const gateways = await many(`
      SELECT id, name, region, status, enabled, latency, last_seen
      FROM gateways
      ORDER BY id DESC
    `);

    return json(res, { success: true, gateways });
  } catch (error) {
    return json(res, {
      success: false,
      error: "Could not load gateways"
    }, 500);
  }
});


/* =========================================================
   GATEWAY SECRET AUTH
========================================================= */

function gatewaySecretValid(req) {
  const configured =
    String(
      process.env.NEXUS_GATEWAY_SECRET ||
      ""
    ).trim();

  const supplied =
    String(
      req.headers[
        "x-nexus-gateway-secret"
      ] || ""
    ).trim();

  if (!configured || !supplied) {
    return false;
  }

  const a =
    Buffer.from(
      configured,
      "utf8"
    );

  const b =
    Buffer.from(
      supplied,
      "utf8"
    );

  if (a.length !== b.length) {
    return false;
  }

  return crypto.timingSafeEqual(
    a,
    b
  );
}

/* =========================================================
   INTERNAL GATEWAY AUTH
========================================================= */

app.get(
  "/api/internal/gateway/auth",
  async (req, res) => {
    try {
      if (!gatewaySecretValid(req)) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Unauthorized gateway"
          },
          401
        );
      }

      const uuid =
        String(
          req.query.uuid || ""
        ).trim();

      const gatewayId =
        Number(
          req.query.gateway_id
        );

      if (
        !uuid ||
        !Number.isInteger(gatewayId)
      ) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Missing uuid or gateway_id"
          },
          400
        );
      }

      const account =
        await one(
          `
          SELECT
            a.id,
            a.username,
            a.display_name,
            a.uuid,
            a.enabled,
            a.data_limit,
            a.data_used,
            a.device_limit,
            a.expires_at,
            a.gateway_id,

            g.enabled AS gateway_enabled,
            g.status AS gateway_status,
            g.worker_url AS gateway_url,
            g.region AS gateway_region

          FROM accounts a

          LEFT JOIN gateways g
            ON g.id = a.gateway_id

          WHERE a.uuid = $1
          LIMIT 1
          `,
          [uuid]
        );

      if (!account) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Account not found"
          },
          403
        );
      }

      if (!account.enabled) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Account disabled"
          },
          403
        );
      }

      if (
        Number(account.gateway_id) !==
        gatewayId
      ) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Gateway assignment mismatch"
          },
          403
        );
      }

      if (
        !account.gateway_enabled
      ) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Gateway disabled"
          },
          403
        );
      }

      if (
        account.gateway_status !==
        "online"
      ) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Gateway is not online"
          },
          503
        );
      }

      if (
        account.expires_at &&
        new Date(
          account.expires_at
        ).getTime() <= Date.now()
      ) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Account expired"
          },
          403
        );
      }

      const limit =
        Number(
          account.data_limit || 0
        );

      const used =
        Number(
          account.data_used || 0
        );

      if (
        limit > 0 &&
        used >= limit
      ) {
        return json(
          res,
          {
            authorized: false,
            error:
              "Data limit reached"
          },
          403
        );
      }

      return json(res, {
        authorized: true,

        account: {
          id: Number(account.id),
          username:
            account.username,
          display_name:
            account.display_name,
          uuid:
            account.uuid,
          data_limit:
            limit,
          data_used:
            used,
          device_limit:
            Number(
              account.device_limit || 0
            ),
          gateway_id:
            Number(account.gateway_id),
          gateway_region:
            account.gateway_region
        }
      });
    } catch (error) {
      console.error(
        "Gateway auth error:",
        error
      );

      return json(
        res,
        {
          authorized: false,
          error:
            "Gateway authorization failed"
        },
        500
      );
    }
  }
);

/* =========================================================
   GATEWAY HEALTH RESULT
========================================================= */

app.post(
  "/api/gateways/:id/health-result",
  async (req, res) => {
    try {
      if (!gatewaySecretValid(req)) {
        return json(
          res,
          {
            success: false,
            error: "Unauthorized"
          },
          401
        );
      }

      const id =
        Number(req.params.id);

      if (!Number.isInteger(id)) {
        return json(
          res,
          {
            success: false,
            error:
              "Invalid gateway id"
          },
          400
        );
      }

      const status =
        String(
          req.body?.status ||
            "online"
        )
          .trim()
          .slice(0, 30);

      const latency =
        req.body?.latency_ms == null
          ? null
          : Math.max(
              0,
              Math.floor(
                toNumber(
                  req.body.latency_ms,
                  0
                )
              )
            );

      const errorMessage =
        req.body?.error
          ? String(
              req.body.error
            ).slice(0, 1000)
          : null;

      const gateway =
        await one(
          `
          UPDATE gateways
          SET
            status = $1,
            latency_ms = $2,
            last_seen =
              CASE
                WHEN $1 = 'online'
                  THEN CURRENT_TIMESTAMP
                ELSE last_seen
              END,
            last_error = $3,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = $4
          RETURNING *
          `,
          [
            status,
            latency,
            errorMessage,
            id
          ]
        );

      if (!gateway) {
        return json(
          res,
          {
            success: false,
            error:
              "Gateway not found"
          },
          404
        );
      }

      return json(res, {
        success: true,
        gateway
      });
    } catch (error) {
      return json(
        res,
        {
          success: false,
          error: error.message
        },
        500
      );
    }
  }
);

/* =========================================================
   CLOUDFLARE API HELPERS
========================================================= */

function cfHeaders() {
  const token =
    String(
      process.env.CF_TOKEN || ""
    ).trim();

  const accountId =
    String(
      process.env.CF_ACCOUNT_ID || ""
    ).trim();

  if (!token) {
    throw new Error(
      "CF_TOKEN is required"
    );
  }

  if (!accountId) {
    throw new Error(
      "CF_ACCOUNT_ID is required"
    );
  }

  return {
    Authorization:
      `Bearer ${token}`,
    "Content-Type":
      "application/json"
  };
}

async function cloudflareRequest(
  endpoint,
  options = {}
) {
  const response =
    await fetch(
      `https://api.cloudflare.com/client/v4${endpoint}`,
      {
        ...options,
        headers: {
          ...cfHeaders(),
          ...(options.headers || {})
        }
      }
    );

  const text =
    await response.text();

  let data = {};

  try {
    data =
      text
        ? JSON.parse(text)
        : {};
  } catch {
    data = {
      success: false,
      error: text
    };
  }

  if (
    !response.ok ||
    data.success === false
  ) {
    const message =
      Array.isArray(data.errors)
        ? data.errors
            .map(
              item =>
                item?.message
            )
            .filter(Boolean)
            .join(", ")
        : "";

    throw new Error(
      message ||
      data.error ||
      `Cloudflare API error: ${response.status}`
    );
  }

  return data;
}

/* =========================================================
   GATEWAY WORKER CODE
========================================================= */



function getGatewayWorkerCode() {
  return `
import { connect } from "cloudflare:sockets";

function json(data, status) {
  return new Response(JSON.stringify(data), {
    status: status || 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store"
    }
  });
}

function uuidFromBytes(bytes) {
  const hex = Array.from(bytes, function (b) {
    return b.toString(16).padStart(2, "0");
  }).join("");

  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20, 32)
  ].join("-");
}

function parseVless(buffer) {
  if (buffer.length < 18) return null;
  
  const version = buffer[0];
  if (version !== 0) throw new Error("Unsupported VLESS version");

  const uuid = uuidFromBytes(buffer.subarray(1, 17));
  const optionsLength = buffer[17];
  const commandIndex = 18 + optionsLength;

  if (buffer.length < commandIndex + 4) return null;

  const command = buffer[commandIndex];
  if (command !== 1) throw new Error("Only TCP is supported");

  const port = buffer.readUInt16BE
    ? buffer.readUInt16BE(commandIndex + 1)
    : (buffer[commandIndex + 1] * 256 +
       buffer[commandIndex + 2]);

  const addressType = buffer[commandIndex + 3];
  let offset = commandIndex + 4;
  let host = "";

  if (addressType === 1) {
    if (buffer.length < offset + 4) return null;
    host = Array.from(buffer.subarray(offset, offset + 4)).join(".");
    offset += 4;
  } else if (addressType === 2) {
    if (buffer.length < offset + 1) return null;
    const length = buffer[offset++];
    if (length < 1) throw new Error("Invalid domain");
    if (buffer.length < offset + length) return null;
    host = new TextDecoder().decode(
      buffer.subarray(offset, offset + length)
    );
    offset += length;
  } else if (addressType === 3) {
    if (buffer.length < offset + 16) return null;
    const parts = [];
    for (let i = 0; i < 16; i += 2) {
      parts.push(
        ((buffer[offset + i] << 8) | buffer[offset + i + 1])
          .toString(16)
      );
    }
    host = parts.join(":");
    offset += 16;
  } else {
    throw new Error("Unsupported address type");
  }

  if (!host || !port) throw new Error("Invalid destination");

  return {
    version: version,
    uuid: uuid,
    host: host,
    port: port,
    offset: offset
  };
}

async function authorize(uuid, env) {
  const base = String(env.NEXUS_API_URL || "")
    .trim()
    .replace(/\\/+$/, "");

  if (!base || !env.NEXUS_GATEWAY_SECRET) {
    throw new Error("Gateway authorization is not configured");
  }

  const url = new URL(base + "/api/internal/gateway/auth");
  url.searchParams.set("uuid", uuid);
  url.searchParams.set("gateway_id", String(env.GATEWAY_ID || ""));

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "X-Nexus-Gateway-Secret": env.NEXUS_GATEWAY_SECRET,
      "Accept": "application/json"
    }
  });

  if (!response.ok) {
    throw new Error("Gateway authorization API returned " + response.status);
  }

  const data = await response.json();
  if (!data || data.authorized !== true) {
    throw new Error("UUID is not authorized");
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type"
        }
      });
    }

    if (path === "/" || path === "/health") {
      return json({
        success: true,
        status: "online",
        gateway: "Trendify Nexus Gateway",
        gateway_id: env.GATEWAY_ID || "",
        region: env.GATEWAY_REGION || ""
      });
    }

    if (
      path !== "/ws" &&
      !path.startsWith("/ws/") &&
      path !== "/vless"
    ) {
      return new Response("Not Found", { status: 404 });
    }

    if (
      !request.headers.get("Upgrade") ||
      request.headers.get("Upgrade").toLowerCase() !== "websocket"
    ) {
      return new Response("Expected WebSocket", {
        status: 426,
        headers: { Upgrade: "websocket" }
      });
    }

    const railwayHost = String(
      env.RAILWAY_TCP_PROXY_DOMAIN || ""
    ).trim();
    const railwayPort = Number(env.RAILWAY_TCP_PROXY_PORT || 0);
    const relaySecret = String(env.NEXUS_GATEWAY_SECRET || "");

    if (!railwayHost || !railwayPort || !relaySecret) {
      return new Response("Gateway relay configuration is incomplete", {
        status: 500
      });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const ws = pair[1];

    ws.accept();
    ws.binaryType = "arraybuffer";

    let pending = new Uint8Array(0);
    let socket = null;
    let writer = null;
    let reader = null;
    let connected = false;
    let closed = false;
    let chain = Promise.resolve();

    function closeAll() {
      if (closed) return;
      closed = true;

      try {
        if (ws.readyState === WebSocket.OPEN) ws.close();
      } catch (_) {}

      try {
        if (reader) reader.cancel();
      } catch (_) {}

      try {
        if (writer) writer.close();
      } catch (_) {}

      try {
        if (socket) socket.close();
      } catch (_) {}
    }

    async function handleMessage(data) {
      if (closed) return;

      let incoming;
      if (data instanceof ArrayBuffer) {
        incoming = new Uint8Array(data);
      } else if (ArrayBuffer.isView(data)) {
        incoming = new Uint8Array(
          data.buffer,
          data.byteOffset,
          data.byteLength
        );
      } else if (data instanceof Blob) {
        incoming = new Uint8Array(await data.arrayBuffer());
      } else {
        throw new Error("Binary WebSocket messages are required");
      }

      if (connected) {
        await writer.write(incoming);
        return;
      }

      const combined = new Uint8Array(pending.length + incoming.length);
      combined.set(pending, 0);
      combined.set(incoming, pending.length);
      pending = combined;

      const parsed = parseVless(pending);
      if (!parsed) {
        if (pending.length > 8192) {
          throw new Error("VLESS header is too large");
        }
        return;
      }

      await authorize(parsed.uuid, env);

      const secretBytes = new TextEncoder().encode(relaySecret);
      const hostBytes = new TextEncoder().encode(parsed.host);

      if (secretBytes.length < 16 || secretBytes.length > 256) {
        throw new Error("Relay secret length must be 16-256 bytes");
      }
      if (hostBytes.length < 1 || hostBytes.length > 255) {
        throw new Error("Destination hostname is too long");
      }

      const payload = pending.subarray(parsed.offset);
      const frame = new Uint8Array(
        2 + secretBytes.length +
        2 + hostBytes.length +
        2 + payload.length
      );

      let p = 0;
      frame[p++] = (secretBytes.length >> 8) & 255;
      frame[p++] = secretBytes.length & 255;
      frame.set(secretBytes, p);
      p += secretBytes.length;

      frame[p++] = (hostBytes.length >> 8) & 255;
      frame[p++] = hostBytes.length & 255;
      frame.set(hostBytes, p);
      p += hostBytes.length;

      frame[p++] = (parsed.port >> 8) & 255;
      frame[p++] = parsed.port & 255;
      frame.set(payload, p);

      socket = connect({
        hostname: railwayHost,
        port: railwayPort
      });

      await socket.opened;
      writer = socket.writable.getWriter();
      reader = socket.readable.getReader();

      await writer.write(frame);
      connected = true;
      pending = new Uint8Array(0);

      // VLESS response: version, zero-length response options.
      ws.send(new Uint8Array([parsed.version, 0]));

      (async function relayToClient() {
        try {
          while (!closed) {
            const result = await reader.read();
            if (result.done) break;
            if (result.value && ws.readyState === WebSocket.OPEN) {
              ws.send(result.value);
            }
          }
        } catch (_) {
          closeAll();
        }
      })();
    }

    ws.addEventListener("message", function (event) {
      chain = chain
        .then(function () {
          return handleMessage(event.data);
        })
        .catch(function () {
          closeAll();
        });
    });

    ws.addEventListener("close", closeAll);
    ws.addEventListener("error", closeAll);

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
};
`;
}

/* =========================================================
   TEST GATEWAY HEALTH
========================================================= */



async function testGatewayHealth(workerUrl) {
  const base = String(workerUrl || "")
    .trim()
    .replace(/\/+$/, "");

  if (!base) {
    throw new Error("Gateway worker URL is empty");
  }

  let lastError = "Gateway health check failed";

  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      const started = Date.now();

      const response = await fetch(`${base}/health`, {
        method: "GET",
        headers: {
          Accept: "application/json",
          "Cache-Control": "no-cache"
        },
        signal: AbortSignal.timeout(10000)
      });

      const latency = Date.now() - started;
      const text = await response.text();

      let data = {};

      try {
        data = text ? JSON.parse(text) : {};
      } catch {}

      if (response.ok && data?.success === true) {
        return {
          latency_ms: latency,
          latency,
          data
        };
      }

      lastError =
        `Gateway health returned HTTP ${response.status}: ${text}`;
    } catch (error) {
      lastError = String(error?.message || error);
    }

    if (attempt < 5) {
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  throw new Error(
    `Gateway health failed after 5 attempts: ${lastError}`
  );
}


/* =========================================================
   CREATE CLOUDFLARE GATEWAY
========================================================= */

app.post(
  "/api/gateways",
  async (req, res) => {
    try {
      if (!(await requireAdmin(req, res))) {
        return;
      }

      if (
        !process.env.CF_TOKEN ||
        !process.env.CF_ACCOUNT_ID
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Cloudflare configuration is missing"
          },
          500
        );
      }

      if (
        !process.env.NEXUS_GATEWAY_SECRET
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "NEXUS_GATEWAY_SECRET is missing"
          },
          500
        );
      }

      const name =
        String(
          req.body?.name ||
            `Gateway ${randomString(5)}`
        )
          .trim()
          .slice(0, 100);

      const region =
        String(
          req.body?.region ||
            "eu"
        )
          .trim()
          .toLowerCase();

      const allowedRegions =
        new Set([
          "eu",
          "us",
          "asia"
        ]);

      if (
        !allowedRegions.has(region)
      ) {
        return json(
          res,
          {
            success: false,
            error:
              "Invalid gateway region. Use eu, us or asia."
          },
          400
        );
      }

      const workerName =
        `nexus-${randomString(12)}`;

      const created =
        await one(
          `
          INSERT INTO gateways
          (
            name,
            worker_name,
            worker_url,
            enabled,
            region,
            status,
            latency_ms,
            last_seen,
            last_error,
            created_at,
            updated_at
          )
          VALUES
          (
            $1,
            $2,
            NULL,
            FALSE,
            $3,
            'creating',
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
          )
          RETURNING *
          `,
          [
            name,
            workerName,
            region
          ]
        );

      try {
        await deployGatewayWorker({
          workerName,
          gatewayId:
            Number(created.id),
          region
        });

        await enableWorkersDev(
          workerName
        );

        const subdomain =
          await getWorkersDevSubdomain();

        if (!subdomain) {
          throw new Error(
            "Cloudflare workers.dev subdomain not found"
          );
        }

        const workerUrl =
          `https://${workerName}.${subdomain}.workers.dev`;

        const health =
          await testGatewayHealth(
            workerUrl
          );

        const gateway =
          await one(
            `
            UPDATE gateways
            SET
              worker_url = $1,
              enabled = TRUE,
              status = 'online',
              latency_ms = $2,
              last_seen =
                CURRENT_TIMESTAMP,
              last_error = NULL,
              updated_at =
                CURRENT_TIMESTAMP
            WHERE id = $3
            RETURNING *
            `,
            [
              workerUrl,
              health.latency_ms,
              Number(created.id)
            ]
          );

        return json(res, {
          success: true,

          gateway: {
            ...gateway,

            id:
              Number(gateway.id),

            enabled:
              Boolean(
                gateway.enabled
              ),

            latency_ms:
              gateway.latency_ms ==
              null
                ? null
                : Number(
                    gateway.latency_ms
                  )
          }
        });
      } catch (deploymentError) {
        await query(
          `
          UPDATE gateways
          SET
            enabled = FALSE,
            status = 'error',
            last_error = $1,
            updated_at =
              CURRENT_TIMESTAMP
          WHERE id = $2
          `,
          [
            String(
              deploymentError?.message ||
              deploymentError
            ).slice(
              0,
              1000
            ),
            Number(created.id)
          ]
        );

        return json(
          res,
          {
            success: false,
            error:
              deploymentError?.message ||
              "Gateway deployment failed",
            gateway_id:
              Number(created.id)
          },
          500
        );
      }
    } catch (error) {
      console.error(
        "Gateway deployment error:",
        error
      );

      return json(
        res,
        {
          success: false,
          error:
            error?.message ||
            "Gateway creation failed"
        },
        500
      );
    }
  }
);
/* =========================================================
   ENABLE WORKERS.DEV
========================================================= */

async function enableWorkersDev(workerName) {
  const accountId = String(process.env.CF_ACCOUNT_ID || "").trim();
  const cfToken = String(process.env.CF_TOKEN || "").trim();

  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}/subdomain`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${cfToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ enabled: true })
    }
  );

  const data = await response.json();

  if (!response.ok || !data.success) {
    throw new Error(JSON.stringify(data.errors || data));
  }

  return data;
}
/* =========================================================
   GET WORKERS.DEV SUBDOMAIN
========================================================= */

async function getWorkersDevSubdomain() {
  const accountId = String(process.env.CF_ACCOUNT_ID || "").trim();

  if (!accountId) {
    throw new Error("CF_ACCOUNT_ID is not configured");
  }

  const data = await cloudflareRequest(
    `/accounts/${accountId}/workers/subdomain`,
    { method: "GET" }
  );

  return data?.result?.subdomain || data?.result?.name || "";
}

/* =========================================================
   CLOUDFLARE WORKER DEPLOYMENT
========================================================= */

async function deployGatewayWorker({
  workerName,
  gatewayId,
  region
}) {
  const accountId =
    String(process.env.CF_ACCOUNT_ID || "").trim();

  const cfToken =
    String(process.env.CF_TOKEN || "").trim();

  const gatewaySecret =
    String(process.env.NEXUS_GATEWAY_SECRET || "").trim();

  if (!accountId) {
    throw new Error(
      "CF_ACCOUNT_ID is required"
    );
  }

  if (!cfToken) {
    throw new Error(
      "CF_TOKEN is required"
    );
  }

  if (!gatewaySecret) {
    throw new Error(
      "NEXUS_GATEWAY_SECRET is required"
    );
  }

  const normalizedRegion =
    String(region || "eu")
      .trim()
      .toLowerCase();

  const placementMap = {
    eu: "gcp:europe-west1",
    us: "gcp:us-east4",
    asia: "gcp:asia-east1"
  };

  const placement =
    placementMap[normalizedRegion];

  if (!placement) {
    throw new Error(
      `Unsupported gateway region: ${normalizedRegion}`
    );
  }

 const nexusApiUrl =
  String(process.env.PUBLIC_URL || "")
    .trim()
    .replace(/\/+$/, "");

if (!nexusApiUrl) {
  throw new Error(
    "PUBLIC_URL is required to deploy a gateway"
  );
}

  const workerCode =
    getGatewayWorkerCode();


  const railwayHost =
    String(process.env.RAILWAY_TCP_PROXY_DOMAIN || "").trim();

  const railwayPort =
    Number(process.env.RAILWAY_TCP_PROXY_PORT || 0);

  if (
    !railwayHost ||
    !Number.isInteger(railwayPort) ||
    railwayPort < 1 ||
    railwayPort > 65535
  ) {
    throw new Error(
      "RAILWAY_TCP_PROXY_DOMAIN and RAILWAY_TCP_PROXY_PORT are required"
    );
  }


  /*
   * -------------------------------------------------------
   * CLOUDFLARE MODULE WORKER METADATA
   * -------------------------------------------------------
   */

  const metadata = {
    main_module: "worker.js",

    compatibility_date:
      "2026-09-14",

    compatibility_flags: [
      "nodejs_compat"
    ],

    placement: {
      mode: "targeted",
      region: placement
    },


    bindings: [
      {
        name: "GATEWAY_ID",
        type: "plain_text",
        text: String(gatewayId)
      },
      {
        name: "GATEWAY_REGION",
        type: "plain_text",
        text: normalizedRegion
      },
      {
        name: "NEXUS_API_URL",
        type: "plain_text",
        text: nexusApiUrl
      },
      {
        name: "NEXUS_GATEWAY_SECRET",
        type: "secret_text",
        text: gatewaySecret
      },
      {
        name: "RAILWAY_TCP_PROXY_DOMAIN",
        type: "plain_text",
        text: railwayHost
      },
      {
        name: "RAILWAY_TCP_PROXY_PORT",
        type: "plain_text",
        text: String(railwayPort)
      }
    ]
  };

  /*
   * -------------------------------------------------------
   * MULTIPART MODULE DEPLOYMENT
   * -------------------------------------------------------
   */

  const form =    new FormData();

  form.append(
    "metadata",
    new Blob(
      [
        JSON.stringify(metadata)
      ],
      {
        type:
          "application/json"
      }
    ),
    "metadata.json"
  );

  form.append(
    "worker.js",
    new Blob(
      [workerCode],
      {
        type:
          "application/javascript+module"
      }
    ),
    "worker.js"
  );

  /*
   * -------------------------------------------------------
   * DEPLOY WORKER
   * -------------------------------------------------------
   */

  const response =
  await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}`,
    {
      method: "PUT",

        headers: {
          Authorization:
            `Bearer ${cfToken}`
        },

        body: form
      }
    );

  const text =
    await response.text();

  let data;

  try {
    data =
      JSON.parse(text);
  } catch {
    data = {
      success:
        response.ok,
      raw: text
    };
  }

  
  console.error("[Gateway Deploy] Cloudflare response:", {
    status: response.status,
    errors: data?.errors || null,
    success: data?.success,
    messages: data?.messages || null
  });

  if (
    !response.ok ||
    data.success === false
  ) {
    throw new Error(
      `Cloudflare Worker deployment failed: ${
        data?.errors
          ? JSON.stringify(data.errors)
          : text
      }`
    );
  }


  console.log(
    `[Gateway] Worker deployed successfully: ${workerName} (${normalizedRegion})`
  );

  return data;
}


/* =========================================================
   CREATE GATEWAY
========================================================= */

app.post(
  "/api/gateways",
  requireAdmin,
  async (req, res, next) => {
    try {
      const {
        region = "eu"
      } = req.body || {};

      const normalizedRegion =
        String(region || "eu")
          .trim()
          .toLowerCase();

      if (
        !["eu", "us", "asia"].includes(
          normalizedRegion
        )
      ) {
        return json(
          res,
          {
            success: false,
            error: "Invalid gateway region"
          },
          400
        );
      }

      if (!process.env.CF_TOKEN) {
        return json(
          res,
          {
            success: false,
            error: "CF_TOKEN is not configured"
          },
          500
        );
      }

      if (!process.env.CF_ACCOUNT_ID) {
        return json(
          res,
          {
            success: false,
            error: "CF_ACCOUNT_ID is not configured"
          },
          500
        );
      }

      if (!process.env.NEXUS_GATEWAY_SECRET) {
        return json(
          res,
          {
            success: false,
            error:
              "NEXUS_GATEWAY_SECRET is not configured"
          },
          500
        );
      }

      const workerName =
        `nexus-${randomString(12)}`;

      const inserted = await one(
        `
        INSERT INTO gateways (
          name,
          region,
          worker_url,
          status,
          latency_ms,
          enabled,
          last_seen,
          last_error
        )
        VALUES (
          $1,
          $2,
          NULL,
          'creating',
          NULL,
          FALSE,
          NULL,
          NULL
        )
        RETURNING *
        `,
        [
          workerName,
          normalizedRegion
        ]
      );

      const gatewayId = inserted.id;

      try {
        await deployGatewayWorker({
          workerName,
          gatewayId,
          region: normalizedRegion
        });

        await enableWorkersDev(
          workerName
        );

        const subdomain =
          await getWorkersDevSubdomain();

        const workerUrl =
          `https://${workerName}.${subdomain}.workers.dev`;

        const health =
          await testGatewayHealth(
            workerUrl
          );

        const updated = await one(
          `
          UPDATE gateways
          SET
            worker_url = $1,
            status = 'online',
            latency_ms = $2,
            enabled = TRUE,
            last_seen = CURRENT_TIMESTAMP,
            last_error = NULL
          WHERE id = $3
          RETURNING *
          `,
          [
            workerUrl,
            health.latency,
            gatewayId
          ]
        );

        return json(
          res,
          {
            success: true,
            gateway: updated
          },
          201
        );
      } catch (error) {
        await one(
          `
          UPDATE gateways
          SET
            status = 'error',
            enabled = FALSE,
            last_error = $1
          WHERE id = $2
          `,
          [
            String(
              error?.message ||
              error
            ).slice(0, 2000),
            gatewayId
          ]
        );

        throw error;
      }
    } catch (error) {
      next(error);
    }
  }
);


/* =========================================================
   SCANNER RESULTS
   Discovery only.
   NEVER used as VLESS server.
========================================================= */

app.get(
  "/api/scanner/results",
  requireAdmin,
  async (req, res, next) => {
    try {
      const rows = await many(
        `
        SELECT
          id,
          ip,
          port,
          latency_ms,
          status,
          source,
          created_at
        FROM scanner_results
        ORDER BY
          latency_ms ASC NULLS LAST,
          created_at DESC
        LIMIT 500
        `
      );

      return json(
        res,
        {
          success: true,
          results: rows
        }
      );
    } catch (error) {
      next(error);
    }
  }
);


app.post(
  "/api/scanner/add",
  requireAdmin,
  async (req, res, next) => {
    try {
      const {
        ip,
        port = 443,
        latency_ms = null,
        status = "discovered",
        source = "manual"
      } = req.body || {};

      const cleanIp =
        String(ip || "").trim();

      const cleanPort =
        Number(port);

      if (!cleanIp) {
        return json(
          res,
          {
            success: false,
            error: "IP is required"
          },
          400
        );
      }

      if (
        !Number.isInteger(cleanPort) ||
        cleanPort < 1 ||
        cleanPort > 65535
      ) {
        return json(
          res,
          {
            success: false,
            error: "Invalid port"
          },
          400
        );
      }

      const row = await one(
        `
        INSERT INTO scanner_results (
          ip,
          port,
          latency_ms,
          status,
          source
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          $5
        )
        RETURNING *
        `,
        [
          cleanIp,
          cleanPort,
          latency_ms === null
            ? null
            : Number(latency_ms),
          String(status),
          String(source)
        ]
      );

      return json(
        res,
        {
          success: true,
          result: row
        },
        201
      );
    } catch (error) {
      next(error);
    }
  }
);


/* =========================================================
   CLOUDFLARE IP SCAN
   Discovery only.
========================================================= */

app.post(
  "/api/scanner/scan",
  requireAdmin,
  async (req, res, next) => {
    try {
      const response =
        await fetch(
          "https://www.cloudflare.com/ips-v4",
          {
            signal:
              AbortSignal.timeout(15000)
          }
        );

      if (!response.ok) {
        throw new Error(
          `Cloudflare IP list HTTP ${response.status}`
        );
      }

      const text =
        await response.text();

      const ranges =
        text
          .split(/\r?\n/)
          .map(x => x.trim())
          .filter(Boolean);

      return json(
        res,
        {
          success: true,
          discovery_only: true,
          ranges
        }
      );
    } catch (error) {
      next(error);
    }
  }
);


/* =========================================================
   VERIFY SCANNER RESULTS
   Discovery only.
========================================================= */

app.post(
  "/api/scanner/verify",
  requireAdmin,
  async (req, res, next) => {
    try {
      const input =
        Array.isArray(req.body?.results)
          ? req.body.results
          : [];

      if (!input.length) {
        return json(
          res,
          {
            success: false,
            error: "No scanner results provided"
          },
          400
        );
      }

      const normalized =
        input
          .map(item => ({
            ip: String(
              item?.ip || ""
            ).trim(),

            port:
              Number(item?.port || 443),

            latency_ms:
              item?.latency_ms == null
                ? null
                : Number(item.latency_ms),

            status:
              String(
                item?.status ||
                "verified"
              ),

            source:
              String(
                item?.source ||
                "scanner"
              )
          }))
          .filter(item => {
            if (!item.ip) {
              return false;
            }

            if (
              !Number.isInteger(item.port) ||
              item.port < 1 ||
              item.port > 65535
            ) {
              return false;
            }

            return true;
          })
          .sort(
            (a, b) =>
              (a.latency_ms ?? 999999) -
              (b.latency_ms ?? 999999)
          )
          .slice(0, 10);

      const inserted = [];

      for (const item of normalized) {
        const row = await one(
          `
          INSERT INTO scanner_results (
            ip,
            port,
            latency_ms,
            status,
            source
          )
          VALUES (
            $1,
            $2,
            $3,
            $4,
            $5
          )
          RETURNING *
          `,
          [
            item.ip,
            item.port,
            item.latency_ms,
            item.status,
            item.source
          ]
        );

        inserted.push(row);
      }

      return json(
        res,
        {
          success: true,
          discovery_only: true,
          results: inserted
        }
      );
    } catch (error) {
      next(error);
    }
  }
);


/* =========================================================
   STATIC FRONTEND
========================================================= */

const publicDir =
  path.join(
    __dirname,
    "public"
  );

app.use(
  express.static(
    publicDir,
    {
      index: false
    }
  )
);


app.get(
  ["/", "/dashboard"],
  (req, res) => {
    const file =
      path.join(
        publicDir,
        "app.html"
      );

    if (
      !fs.existsSync(file)
    ) {
      return res
        .status(404)
        .send(
          "public/app.html not found"
        );
    }

    return res.sendFile(file);
  }
);


/* =========================================================
   404
========================================================= */

app.use(
  (req, res) => {
    return json(
      res,
      {
        success: false,
        error: "Not Found"
      },
      404
    );
  }
);


/* =========================================================
   ERROR HANDLER
========================================================= */

app.use(
  (error, req, res, next) => {
    console.error(
      "[SERVER ERROR]",
      error
    );

    if (res.headersSent) {
      return next(error);
    }

    return json(
      res,
      {
        success: false,
        error:
          String(
            error?.message ||
            error ||
            "Internal Server Error"
          )
      },
      500
    );
  }
);


/* =========================================================
   START SERVER
========================================================= */

async function start() {
  await initDatabase();

  app.listen(
    PORT,
    "0.0.0.0",
    () => {
      console.log(
        `Trendify Nexus running on port ${PORT}`
      );
    }
  );
}


start().catch(error => {
  console.error(
    "Trendify Nexus startup failed:",
    error
  );

  process.exit(1);
});
