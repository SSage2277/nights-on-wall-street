import dotenv from "dotenv";
import express from "express";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import connectPgSimple from "connect-pg-simple";
import bcrypt from "bcryptjs";
import session from "express-session";
import { Pool } from "pg";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = Number(process.env.PORT || 3000);
const baseUrl = process.env.APP_BASE_URL || `http://localhost:${port}`;
const ADMIN_CODE = String(process.env.VENMO_ADMIN_CODE || "Sage1557");
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const SESSION_SECRET = String(process.env.SESSION_SECRET || "").trim();

if (!DATABASE_URL) {
  throw new Error("Missing DATABASE_URL environment variable.");
}
if (!SESSION_SECRET) {
  throw new Error("Missing SESSION_SECRET environment variable.");
}

const db = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false }
});

const STORE_PACKAGES = Object.freeze({
  small: { usd: 2, funds: 1000, label: "Starter Funds Pack" },
  medium: { usd: 8, funds: 5000, label: "Trader Funds Pack" },
  xlarge: { usd: 18, funds: 10000, label: "Pro Funds Pack" },
  large: { usd: 40, funds: 25000, label: "Whale Funds Pack" }
});
const PgSessionStore = connectPgSimple(session);
const PACKAGE_ALIASES = Object.freeze({
  xl: "xlarge",
  "x-large": "xlarge",
  "extra-large": "xlarge",
  pro: "xlarge",
  whale: "large",
  starter: "small",
  trader: "medium"
});

function normalizeTxn(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "");
}

function sanitizePlayerId(value) {
  const id = String(value || "").trim();
  if (!id) return "";
  if (!/^[a-zA-Z0-9_-]{6,120}$/.test(id)) return "";
  return id;
}

function sanitizeUsername(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "";
  if (normalized.length < 3 || normalized.length > 16) return "";
  return normalized;
}

function normalizeUsernameKey(value) {
  return sanitizeUsername(value).toLowerCase();
}

function sanitizeEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  if (!email) return "";
  const isValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  return isValid ? email : "";
}

function sanitizePassword(value) {
  const password = String(value || "");
  if (!password) return "";
  if (password.length < 8 || password.length > 72) return "";
  return password;
}

function sanitizeClaimSource(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  if (!normalized) return "venmo";
  return normalized.slice(0, 24);
}

function normalizePackageId(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (STORE_PACKAGES[raw]) return raw;
  const mapped = PACKAGE_ALIASES[raw];
  return mapped && STORE_PACKAGES[mapped] ? mapped : "";
}

function sanitizeGameLabel(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "Casino";
  return normalized.slice(0, 24);
}

function claimWithPack(claim) {
  const pack = STORE_PACKAGES[claim.packId] || STORE_PACKAGES[claim.pack_id] || null;
  return {
    ...claim,
    id: String(claim.id),
    funds: pack?.funds || 0,
    usd: pack?.usd || 0
  };
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function mapClaimRow(row) {
  return claimWithPack({
    id: String(row.id),
    playerId: String(row.player_id || ""),
    username: String(row.username || ""),
    email: sanitizeEmail(row.email) || "",
    source: String(row.source || "venmo"),
    packId: String(row.pack_id || ""),
    txnId: String(row.txn_id || ""),
    txnNorm: String(row.txn_norm || ""),
    status: String(row.status || "pending"),
    submittedAt: Number(row.submitted_at) || 0,
    reviewedAt: Number(row.reviewed_at) || 0,
    creditedAt: Number(row.credited_at) || 0
  });
}

function mapWinRow(row) {
  const multiplierValue = row.multiplier === null || row.multiplier === undefined ? null : Number(row.multiplier);
  return {
    id: String(row.id),
    playerId: String(row.player_id || ""),
    username: String(row.username || ""),
    vip: row.vip === true,
    game: String(row.game || "Casino"),
    amount: Math.round(toNumber(row.amount) * 100) / 100,
    multiplier: Number.isFinite(multiplierValue) ? Math.round(multiplierValue * 100) / 100 : null,
    submittedAt: Number(row.submitted_at) || 0
  };
}

function mapUserRow(row) {
  return {
    playerId: String(row.player_id || ""),
    username: sanitizeUsername(row.username) || "Unknown",
    email: sanitizeEmail(row.email) || "",
    usernameKey: String(row.username_key || ""),
    balance: Math.round(toNumber(row.balance) * 100) / 100,
    lastSeenAt: Number(row.last_seen_at) || 0
  };
}

function mapAdminUserRow(row) {
  const base = mapUserRow(row);
  return {
    ...base,
    hasPassword: row.has_password === true,
    totalClaims: Number(row.total_claims) || 0,
    pendingClaims: Number(row.pending_claims) || 0,
    approvedClaims: Number(row.approved_claims) || 0,
    totalWins: Number(row.total_wins) || 0,
    lastWinAt: Number(row.last_win_at) || 0
  };
}

function normalizeUserForClient(row) {
  const mapped = mapUserRow(row);
  return {
    playerId: mapped.playerId,
    username: mapped.username,
    email: mapped.email,
    balance: mapped.balance,
    lastSeenAt: mapped.lastSeenAt
  };
}

function getAdminCode(req) {
  return String(req.query?.adminCode || req.body?.adminCode || "");
}

function sanitizeAdminDeviceToken(value) {
  const token = String(value || "").trim();
  if (!token) return "";
  if (token.length < 24 || token.length > 256) return "";
  if (!/^[a-zA-Z0-9._-]+$/.test(token)) return "";
  return token;
}

function sanitizeAdminDeviceLabel(value) {
  const label = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!label) return "Trusted device";
  return label.slice(0, 80);
}

function hashAdminDeviceToken(token) {
  return crypto.createHash("sha256").update(String(token)).digest("hex");
}

function getAdminDeviceToken(req) {
  const rawHeader = req.headers["x-admin-device-token"];
  const headerToken = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;
  return sanitizeAdminDeviceToken(
    headerToken ||
    req.query?.adminDeviceToken ||
    req.body?.adminDeviceToken
  );
}

function getRequestIp(req) {
  const forwardedRaw = req.headers["x-forwarded-for"];
  const forwarded = Array.isArray(forwardedRaw) ? forwardedRaw[0] : String(forwardedRaw || "");
  const firstForwardedIp = forwarded.split(",")[0]?.trim();
  const fallbackIp = String(req.ip || req.socket?.remoteAddress || "").trim();
  return String(firstForwardedIp || fallbackIp || "").slice(0, 128);
}

function getRequestUserAgent(req) {
  return String(req.headers["user-agent"] || "").slice(0, 255);
}

async function getTrustedAdminDeviceCount() {
  const result = await db.query(
    `
      SELECT COUNT(*)::int AS count
      FROM admin_trusted_devices
      WHERE revoked_at = 0
        AND token_hash IS NOT NULL
        AND token_hash <> ''
    `
  );
  return Number(result.rows?.[0]?.count) || 0;
}

function mapAdminDeviceRow(row) {
  return {
    id: String(row.id || ""),
    label: sanitizeAdminDeviceLabel(row.label),
    trustedAt: Number(row.created_at) || 0,
    lastSeenAt: Number(row.last_seen_at) || 0,
    lastIp: String(row.last_ip || ""),
    lastUserAgent: String(row.last_user_agent || "")
  };
}

async function touchTrustedAdminDeviceByHash(tokenHash, req) {
  await db.query(
    `
      UPDATE admin_trusted_devices
      SET last_seen_at = $1,
          last_ip = $2,
          last_user_agent = $3
      WHERE token_hash = $4
    `,
    [Date.now(), getRequestIp(req), getRequestUserAgent(req), tokenHash]
  );
}

async function requireAdminAccess(req, res, { allowBootstrap = false } = {}) {
  const entered = getAdminCode(req).trim().toLowerCase();
  const expected = ADMIN_CODE.trim().toLowerCase();
  if (!entered || entered !== expected) {
    res.status(401).json({ ok: false, error: "Invalid admin code." });
    return null;
  }
  try {
    const trustedCount = await getTrustedAdminDeviceCount();
    const deviceToken = getAdminDeviceToken(req);
    if (trustedCount <= 0 && allowBootstrap) {
      return {
        ok: true,
        bootstrap: true,
        trustedCount: 0,
        token: deviceToken,
        tokenHash: deviceToken ? hashAdminDeviceToken(deviceToken) : ""
      };
    }
    if (!deviceToken) {
      res.status(401).json({ ok: false, error: "Trusted admin device required." });
      return null;
    }
    const tokenHash = hashAdminDeviceToken(deviceToken);
    const lookup = await db.query(
      `
        SELECT id, label, created_at, last_seen_at, last_ip, last_user_agent
        FROM admin_trusted_devices
        WHERE token_hash = $1
          AND revoked_at = 0
        LIMIT 1
      `,
      [tokenHash]
    );
    if (!lookup.rowCount) {
      res.status(401).json({ ok: false, error: "This device is not trusted for admin access." });
      return null;
    }
    await touchTrustedAdminDeviceByHash(tokenHash, req);
    return {
      ok: true,
      bootstrap: false,
      trustedCount,
      token: deviceToken,
      tokenHash,
      device: mapAdminDeviceRow(lookup.rows[0])
    };
  } catch (error) {
    console.error("Failed admin access validation", error);
    res.status(500).json({ ok: false, error: "Could not validate admin access." });
    return null;
  }
}

app.use(express.json());
app.use((req, res, next) => {
  const requestOrigin = String(req.headers.origin || "").trim();
  if (requestOrigin) {
    res.setHeader("Access-Control-Allow-Origin", requestOrigin);
    res.setHeader("Vary", "Origin");
  } else {
    res.setHeader("Access-Control-Allow-Origin", "*");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Admin-Device-Token");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }
  next();
});
app.set("trust proxy", 1);
app.use(
  session({
    name: "nows.sid",
    store: new PgSessionStore({
      pool: db,
      tableName: "user_sessions",
      createTableIfMissing: true
    }),
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
      maxAge: 1000 * 60 * 60 * 24 * 30
    }
  })
);
app.use(express.static(__dirname));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "claims", database: "postgres" });
});

function getSessionPlayerId(req) {
  return sanitizePlayerId(req.session?.playerId);
}

app.get("/api/auth/session", async (req, res) => {
  try {
    const playerId = getSessionPlayerId(req);
    if (!playerId) {
      res.json({ ok: true, authenticated: false });
      return;
    }
    const lookup = await db.query(
      `
        SELECT player_id, username, username_key, email, balance, last_seen_at
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (!lookup.rowCount) {
      req.session.destroy(() => {});
      res.json({ ok: true, authenticated: false });
      return;
    }
    res.json({
      ok: true,
      authenticated: true,
      user: normalizeUserForClient(lookup.rows[0])
    });
  } catch (error) {
    console.error("Failed to read auth session", error);
    res.status(500).json({ ok: false, error: "Could not load auth session." });
  }
});

app.post("/api/auth/register", async (req, res) => {
  try {
    const email = sanitizeEmail(req.body?.email);
    const password = sanitizePassword(req.body?.password);
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    const balance = Number(req.body?.balance);
    const providedPlayerId = sanitizePlayerId(req.body?.playerId);
    const playerId = providedPlayerId || `u_${crypto.randomUUID().replace(/-/g, "")}`;
    if (!email) {
      res.status(400).json({ ok: false, error: "Invalid email address." });
      return;
    }
    if (!password) {
      res.status(400).json({ ok: false, error: "Password must be 8-72 characters." });
      return;
    }
    if (!username || !usernameKey) {
      res.status(400).json({ ok: false, error: "Invalid username." });
      return;
    }
    const safeBalance = Number.isFinite(balance) && balance >= 0 ? Math.round(balance * 100) / 100 : 0;
    const passwordHash = await bcrypt.hash(password, 12);
    const existingByPlayerId = await db.query(
      `
        SELECT player_id, username, username_key, email, password_hash, balance, last_seen_at
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (existingByPlayerId.rowCount) {
      const existing = existingByPlayerId.rows[0];
      const hasCredentials = Boolean(sanitizeEmail(existing.email) && String(existing.password_hash || "").trim());
      if (hasCredentials) {
        const existingEmail = sanitizeEmail(existing.email);
        if (existingEmail !== email) {
          res.status(409).json({ ok: false, error: "Account already exists for this player. Please login." });
          return;
        }
        const passwordMatch = await bcrypt.compare(password, String(existing.password_hash || ""));
        if (!passwordMatch) {
          res.status(401).json({ ok: false, error: "Invalid email or password." });
          return;
        }
        const now = Date.now();
        const refreshed = await db.query(
          `
            UPDATE users
            SET last_seen_at = $1
            WHERE player_id = $2
            RETURNING player_id, username, username_key, email, balance, last_seen_at
          `,
          [now, playerId]
        );
        req.session.playerId = playerId;
        res.json({
          ok: true,
          user: normalizeUserForClient(refreshed.rows[0] || existing)
        });
        return;
      }
    }
    const conflict = await db.query(
      `
        SELECT player_id
        FROM users
        WHERE username_key = $1
          AND player_id <> $2
        LIMIT 1
      `,
      [usernameKey, playerId]
    );
    if (conflict.rowCount) {
      res.status(409).json({ ok: false, error: "Username already taken." });
      return;
    }
    const upsert = await db.query(
      `
        INSERT INTO users (player_id, username, username_key, email, password_hash, balance, last_seen_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (player_id)
        DO UPDATE SET
          username = EXCLUDED.username,
          username_key = EXCLUDED.username_key,
          email = EXCLUDED.email,
          password_hash = EXCLUDED.password_hash,
          balance = CASE
            WHEN users.balance > EXCLUDED.balance THEN users.balance
            ELSE EXCLUDED.balance
          END,
          last_seen_at = EXCLUDED.last_seen_at
        RETURNING player_id, username, username_key, email, balance, last_seen_at
      `,
      [playerId, username, usernameKey, email, passwordHash, safeBalance, Date.now()]
    );
    req.session.playerId = playerId;
    res.json({
      ok: true,
      user: normalizeUserForClient(upsert.rows[0])
    });
  } catch (error) {
    if (error?.code === "23505") {
      const message = String(error?.constraint || "").includes("email")
        ? "Email already in use."
        : "Username already taken.";
      res.status(409).json({ ok: false, error: message });
      return;
    }
    console.error("Failed to register auth user", error);
    res.status(500).json({ ok: false, error: "Could not register account." });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const email = sanitizeEmail(req.body?.email);
    const password = sanitizePassword(req.body?.password);
    if (!email || !password) {
      res.status(400).json({ ok: false, error: "Invalid email or password." });
      return;
    }
    const lookup = await db.query(
      `
        SELECT player_id, username, username_key, email, password_hash, balance, last_seen_at
        FROM users
        WHERE lower(email) = lower($1)
        LIMIT 1
      `,
      [email]
    );
    if (!lookup.rowCount) {
      res.status(401).json({ ok: false, error: "Invalid email or password." });
      return;
    }
    const user = lookup.rows[0];
    const storedHash = String(user.password_hash || "");
    if (!storedHash) {
      res.status(401).json({ ok: false, error: "Account password is not set." });
      return;
    }
    const isMatch = await bcrypt.compare(password, storedHash);
    if (!isMatch) {
      res.status(401).json({ ok: false, error: "Invalid email or password." });
      return;
    }
    await db.query(
      `
        UPDATE users
        SET last_seen_at = $1
        WHERE player_id = $2
      `,
      [Date.now(), user.player_id]
    );
    req.session.playerId = sanitizePlayerId(user.player_id);
    res.json({ ok: true, user: normalizeUserForClient(user) });
  } catch (error) {
    console.error("Failed to login", error);
    res.status(500).json({ ok: false, error: "Could not login." });
  }
});

app.post("/api/auth/logout", (req, res) => {
  req.session.destroy((error) => {
    if (error) {
      console.error("Failed to logout", error);
      res.status(500).json({ ok: false, error: "Could not logout." });
      return;
    }
    res.clearCookie("nows.sid");
    res.json({ ok: true });
  });
});

app.get("/api/live-wins", async (req, res) => {
  try {
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(120, Math.floor(rawLimit))) : 40;
    const result = await db.query(
      `
        SELECT id, player_id, username, vip, game, amount, multiplier, submitted_at
        FROM live_wins
        ORDER BY submitted_at DESC, id DESC
        LIMIT $1
      `,
      [limit]
    );
    const wins = result.rows.map(mapWinRow);
    res.json({ ok: true, wins });
  } catch (error) {
    console.error("Failed to load live wins", error);
    res.status(500).json({ ok: false, error: "Could not load live wins." });
  }
});

app.post("/api/live-wins", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.body?.playerId);
    const username = sanitizeUsername(req.body?.username);
    const game = sanitizeGameLabel(req.body?.game);
    const amount = Math.round((Number(req.body?.amount) || 0) * 100) / 100;
    const multiplierRaw = Number(req.body?.multiplier);
    const vip = req.body?.vip === true;
    if (!playerId || !username || !Number.isFinite(amount) || amount <= 0) {
      res.status(400).json({ ok: false, error: "Invalid live win payload." });
      return;
    }
    const multiplier = Number.isFinite(multiplierRaw) && multiplierRaw > 0
      ? Math.round(multiplierRaw * 100) / 100
      : null;
    const submittedAt = Date.now();
    const insertResult = await db.query(
      `
        INSERT INTO live_wins (player_id, username, vip, game, amount, multiplier, submitted_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, player_id, username, vip, game, amount, multiplier, submitted_at
      `,
      [playerId, username, vip, game, amount, multiplier, submittedAt]
    );
    await db.query(
      `
        DELETE FROM live_wins
        WHERE id IN (
          SELECT id
          FROM live_wins
          ORDER BY submitted_at DESC, id DESC
          OFFSET 300
        )
      `
    );
    const win = mapWinRow(insertResult.rows[0]);
    res.json({ ok: true, win });
  } catch (error) {
    console.error("Failed to submit live win", error);
    res.status(500).json({ ok: false, error: "Could not submit live win." });
  }
});

app.post("/api/claims", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.body?.playerId);
    const username = sanitizeUsername(req.body?.username);
    const packageId = normalizePackageId(
      req.body?.packageId ||
      req.body?.packId ||
      req.body?.pack ||
      req.body?.package
    );
    const source = sanitizeClaimSource(req.body?.source);
    const txnId = String(req.body?.txnId || "").trim();
    const txnNorm = normalizeTxn(txnId);
    const pack = STORE_PACKAGES[packageId];
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    if (!pack) {
      res.status(400).json({ ok: false, error: "Invalid package." });
      return;
    }
    if (txnNorm.length < 4) {
      res.status(400).json({ ok: false, error: "Invalid transaction id." });
      return;
    }
    const submittedAt = Date.now();
    const insertResult = await db.query(
      `
        INSERT INTO claims (player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, 0, 0)
        RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
      `,
      [playerId, username || playerId, source, packageId, txnId, txnNorm, submittedAt]
    );
    res.json({ ok: true, claim: mapClaimRow(insertResult.rows[0]) });
  } catch (error) {
    if (error?.code === "23505") {
      res.status(409).json({ ok: false, error: "Transaction id already claimed." });
      return;
    }
    console.error("Failed to submit claim", error);
    res.status(500).json({ ok: false, error: "Could not submit claim." });
  }
});

app.get("/api/claims", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.query?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Missing or invalid playerId." });
      return;
    }
    const result = await db.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE player_id = $1
        ORDER BY submitted_at DESC, id DESC
      `,
      [playerId]
    );
    const claims = result.rows.map(mapClaimRow);
    res.json({
      ok: true,
      claims
    });
  } catch (error) {
    console.error("Failed to list claims", error);
    res.status(500).json({ ok: false, error: "Could not load claims." });
  }
});

app.get("/api/claims/credits", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.query?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Missing or invalid playerId." });
      return;
    }
    const result = await db.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE player_id = $1
          AND status = 'approved'
          AND credited_at = 0
        ORDER BY submitted_at DESC, id DESC
      `,
      [playerId]
    );
    const claims = result.rows.map(mapClaimRow);
    res.json({ ok: true, claims });
  } catch (error) {
    console.error("Failed to list credits", error);
    res.status(500).json({ ok: false, error: "Could not load approved credits." });
  }
});

app.post("/api/claims/credits/:id/ack", async (req, res) => {
  try {
    const claimId = String(req.params?.id || "");
    const playerId = sanitizePlayerId(req.body?.playerId);
    if (!claimId || !playerId) {
      res.status(400).json({ ok: false, error: "Missing claim id or player id." });
      return;
    }
    const lookup = await db.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE id = $1 AND player_id = $2
        LIMIT 1
      `,
      [claimId, playerId]
    );
    if (!lookup.rowCount) {
      res.status(404).json({ ok: false, error: "Claim not found." });
      return;
    }
    const claim = lookup.rows[0];
    if (claim.status !== "approved") {
      res.status(409).json({ ok: false, error: "Claim is not approved." });
      return;
    }
    if (!Number(claim.credited_at)) {
      const updated = await db.query(
        `
          UPDATE claims
          SET credited_at = $1
          WHERE id = $2
          RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        `,
        [Date.now(), claimId]
      );
      res.json({ ok: true, claim: mapClaimRow(updated.rows[0]) });
      return;
    }
    res.json({ ok: true, claim: mapClaimRow(claim) });
  } catch (error) {
    console.error("Failed to ack credit", error);
    res.status(500).json({ ok: false, error: "Could not acknowledge credit." });
  }
});

app.post("/api/users/sync", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.body?.playerId);
    const sessionPlayerId = getSessionPlayerId(req);
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    const balance = Number(req.body?.balance);
    if (!playerId || !username || !usernameKey || !Number.isFinite(balance) || balance < 0) {
      res.status(400).json({ ok: false, error: "Invalid user sync payload." });
      return;
    }
    if (sessionPlayerId && sessionPlayerId !== playerId) {
      res.status(403).json({ ok: false, error: "Session does not match this player." });
      return;
    }
    const conflict = await db.query(
      `
        SELECT player_id
        FROM users
        WHERE username_key = $1
          AND player_id <> $2
        LIMIT 1
      `,
      [usernameKey, playerId]
    );
    if (conflict.rowCount) {
      res.status(409).json({ ok: false, error: "Username already taken." });
      return;
    }
    const upsert = await db.query(
      `
        INSERT INTO users (player_id, username, username_key, balance, last_seen_at, email, password_hash)
        VALUES ($1, $2, $3, $4, $5, NULL, NULL)
        ON CONFLICT (player_id)
        DO UPDATE SET
          username = EXCLUDED.username,
          username_key = EXCLUDED.username_key,
          balance = EXCLUDED.balance,
          last_seen_at = EXCLUDED.last_seen_at
        RETURNING player_id, username, username_key, email, balance, last_seen_at
      `,
      [playerId, username, usernameKey, Math.round(balance * 100) / 100, Date.now()]
    );
    const user = mapUserRow(upsert.rows[0]);
    res.json({ ok: true, user });
  } catch (error) {
    if (error?.code === "23505") {
      res.status(409).json({ ok: false, error: "Username already taken." });
      return;
    }
    console.error("Failed to sync user profile", error);
    res.status(500).json({ ok: false, error: "Could not sync user profile." });
  }
});

app.get("/api/admin/claims", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const statusFilter = String(req.query?.status || "").trim().toLowerCase();
    const result = statusFilter
      ? await db.query(
          `
            SELECT c.id, c.player_id, c.username, c.source, c.pack_id, c.txn_id, c.txn_norm, c.status, c.submitted_at, c.reviewed_at, c.credited_at, u.email
            FROM claims c
            LEFT JOIN users u ON u.player_id = c.player_id
            WHERE status = $1
            ORDER BY c.submitted_at DESC, c.id DESC
          `,
          [statusFilter]
        )
      : await db.query(
          `
            SELECT c.id, c.player_id, c.username, c.source, c.pack_id, c.txn_id, c.txn_norm, c.status, c.submitted_at, c.reviewed_at, c.credited_at, u.email
            FROM claims c
            LEFT JOIN users u ON u.player_id = c.player_id
            ORDER BY c.submitted_at DESC, c.id DESC
          `
        );
    const claims = result.rows.map(mapClaimRow);
    res.json({ ok: true, claims, trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to load admin claims", error);
    res.status(500).json({ ok: false, error: "Could not load admin claims." });
  }
});

app.get("/api/admin/users", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const result = await db.query(
      `
        SELECT
          u.player_id,
          u.username,
          u.username_key,
          u.email,
          u.balance,
          u.last_seen_at,
          (u.password_hash IS NOT NULL AND u.password_hash <> '') AS has_password,
          COALESCE(claims.total_claims, 0) AS total_claims,
          COALESCE(claims.pending_claims, 0) AS pending_claims,
          COALESCE(claims.approved_claims, 0) AS approved_claims,
          COALESCE(wins.total_wins, 0) AS total_wins,
          COALESCE(wins.last_win_at, 0) AS last_win_at
        FROM users u
        LEFT JOIN (
          SELECT
            player_id,
            COUNT(*)::int AS total_claims,
            COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_claims,
            COUNT(*) FILTER (WHERE status = 'approved')::int AS approved_claims
          FROM claims
          GROUP BY player_id
        ) claims ON claims.player_id = u.player_id
        LEFT JOIN (
          SELECT
            player_id,
            COUNT(*)::int AS total_wins,
            MAX(submitted_at)::bigint AS last_win_at
          FROM live_wins
          GROUP BY player_id
        ) wins ON wins.player_id = u.player_id
        ORDER BY u.last_seen_at DESC, u.player_id DESC
      `
    );
    const users = result.rows.map(mapAdminUserRow);
    res.json({ ok: true, users, trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to load admin users", error);
    res.status(500).json({ ok: false, error: "Could not load users." });
  }
});

app.post("/api/admin/claims/:id/decision", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const claimId = String(req.params?.id || "");
    const decision = String(req.body?.decision || "").toLowerCase();
    if (!["approve", "reject"].includes(decision)) {
      res.status(400).json({ ok: false, error: "Decision must be approve or reject." });
      return;
    }
    const updated = await db.query(
      `
        UPDATE claims
        SET status = $1, reviewed_at = $2
        WHERE id = $3
          AND status = 'pending'
        RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
      `,
      [decision === "approve" ? "approved" : "rejected", Date.now(), claimId]
    );
    if (!updated.rowCount) {
      const exists = await db.query("SELECT id FROM claims WHERE id = $1 LIMIT 1", [claimId]);
      if (!exists.rowCount) {
        res.status(404).json({ ok: false, error: "Claim not found." });
        return;
      }
      res.status(409).json({ ok: false, error: "Claim is already reviewed." });
      return;
    }
    res.json({ ok: true, claim: mapClaimRow(updated.rows[0]), trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to update claim decision", error);
    res.status(500).json({ ok: false, error: "Could not update claim decision." });
  }
});

app.post("/api/admin/device/trust", async (req, res) => {
  try {
    const adminCode = getAdminCode(req).trim().toLowerCase();
    const expected = ADMIN_CODE.trim().toLowerCase();
    if (!adminCode || adminCode !== expected) {
      res.status(401).json({ ok: false, error: "Invalid admin code." });
      return;
    }
    const deviceToken = getAdminDeviceToken(req);
    if (!deviceToken) {
      res.status(400).json({ ok: false, error: "Missing admin device token." });
      return;
    }
    const trustedCount = await getTrustedAdminDeviceCount();
    if (trustedCount > 0) {
      const guard = await requireAdminAccess(req, res, { allowBootstrap: false });
      if (!guard) return;
    }
    const tokenHash = hashAdminDeviceToken(deviceToken);
    const label = sanitizeAdminDeviceLabel(req.body?.label);
    const now = Date.now();
    const upsert = await db.query(
      `
        INSERT INTO admin_trusted_devices (token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at)
        VALUES ($1, $2, $3, $3, $4, $5, 0)
        ON CONFLICT (token_hash)
        DO UPDATE SET
          label = EXCLUDED.label,
          last_seen_at = EXCLUDED.last_seen_at,
          last_ip = EXCLUDED.last_ip,
          last_user_agent = EXCLUDED.last_user_agent,
          revoked_at = 0
        RETURNING id, label, created_at, last_seen_at, last_ip, last_user_agent
      `,
      [tokenHash, label, now, getRequestIp(req), getRequestUserAgent(req)]
    );
    res.json({
      ok: true,
      bootstrap: trustedCount <= 0,
      trustedDevice: mapAdminDeviceRow(upsert.rows[0])
    });
  } catch (error) {
    console.error("Failed to trust admin device", error);
    res.status(500).json({ ok: false, error: "Could not trust this device." });
  }
});

app.get("/api/admin/devices", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const currentToken = getAdminDeviceToken(req);
    const currentTokenHash = currentToken ? hashAdminDeviceToken(currentToken) : "";
    const result = await db.query(
      `
        SELECT id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent
        FROM admin_trusted_devices
        WHERE revoked_at = 0
          AND token_hash IS NOT NULL
          AND token_hash <> ''
        ORDER BY created_at DESC, id DESC
      `
    );
    const devices = result.rows.map((row) => ({
      ...mapAdminDeviceRow(row),
      current: Boolean(currentTokenHash && row.token_hash === currentTokenHash)
    }));
    res.json({ ok: true, devices });
  } catch (error) {
    console.error("Failed to load admin devices", error);
    res.status(500).json({ ok: false, error: "Could not load trusted devices." });
  }
});

app.get("/api/admin/stats", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const dayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const totals = await db.query(
      `
        WITH user_totals AS (
          SELECT
            COUNT(*)::int AS total_users,
            COUNT(*) FILTER (WHERE email IS NOT NULL AND email <> '')::int AS users_with_email,
            COUNT(*) FILTER (WHERE password_hash IS NOT NULL AND password_hash <> '')::int AS users_with_password,
            COUNT(*) FILTER (WHERE last_seen_at >= $1)::int AS active_users_24h,
            COALESCE(SUM(balance), 0)::numeric AS total_balance
          FROM users
        ),
        claim_totals AS (
          SELECT
            COUNT(*)::int AS total_claims,
            COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_claims,
            COUNT(*) FILTER (WHERE status = 'approved')::int AS approved_claims,
            COUNT(*) FILTER (WHERE status = 'rejected')::int AS rejected_claims
          FROM claims
        ),
        win_totals AS (
          SELECT
            COUNT(*) FILTER (WHERE submitted_at >= $1)::int AS live_wins_24h,
            COALESCE(SUM(amount) FILTER (WHERE submitted_at >= $1), 0)::numeric AS live_win_amount_24h
          FROM live_wins
        )
        SELECT
          user_totals.total_users,
          user_totals.users_with_email,
          user_totals.users_with_password,
          user_totals.active_users_24h,
          user_totals.total_balance,
          claim_totals.total_claims,
          claim_totals.pending_claims,
          claim_totals.approved_claims,
          claim_totals.rejected_claims,
          win_totals.live_wins_24h,
          win_totals.live_win_amount_24h
        FROM user_totals, claim_totals, win_totals
      `,
      [dayAgo]
    );
    const row = totals.rows[0] || {};
    res.json({
      ok: true,
      stats: {
        totalUsers: Number(row.total_users) || 0,
        usersWithEmail: Number(row.users_with_email) || 0,
        usersWithPassword: Number(row.users_with_password) || 0,
        activeUsers24h: Number(row.active_users_24h) || 0,
        totalBalance: Math.round(toNumber(row.total_balance) * 100) / 100,
        totalClaims: Number(row.total_claims) || 0,
        pendingClaims: Number(row.pending_claims) || 0,
        approvedClaims: Number(row.approved_claims) || 0,
        rejectedClaims: Number(row.rejected_claims) || 0,
        liveWins24h: Number(row.live_wins_24h) || 0,
        liveWinAmount24h: Math.round(toNumber(row.live_win_amount_24h) * 100) / 100
      },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load admin stats", error);
    res.status(500).json({ ok: false, error: "Could not load admin stats." });
  }
});

async function ensureSchema() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      player_id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      username_key TEXT NOT NULL UNIQUE,
      email TEXT,
      password_hash TEXT,
      balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      last_seen_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email TEXT
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS password_hash TEXT
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS username_key TEXT
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS balance NUMERIC(14,2) NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS last_seen_at BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    UPDATE users
    SET username_key = lower(username)
    WHERE username_key IS NULL OR username_key = ''
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS users_username_key_idx
    ON users (username_key)
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique_idx
    ON users (lower(email))
    WHERE email IS NOT NULL
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_trusted_devices (
      id BIGSERIAL PRIMARY KEY,
      token_hash TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL DEFAULT 'Trusted device',
      created_at BIGINT NOT NULL DEFAULT 0,
      last_seen_at BIGINT NOT NULL DEFAULT 0,
      last_ip TEXT NOT NULL DEFAULT '',
      last_user_agent TEXT NOT NULL DEFAULT '',
      revoked_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS token_hash TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS label TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS created_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS last_seen_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS last_ip TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS last_user_agent TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS revoked_at BIGINT
  `);
  await db.query(`
    UPDATE admin_trusted_devices
    SET label = COALESCE(NULLIF(label, ''), 'Trusted device'),
        created_at = COALESCE(created_at, 0),
        last_seen_at = COALESCE(last_seen_at, 0),
        last_ip = COALESCE(last_ip, ''),
        last_user_agent = COALESCE(last_user_agent, ''),
        revoked_at = COALESCE(revoked_at, 0)
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS admin_trusted_devices_token_hash_idx
    ON admin_trusted_devices (token_hash)
    WHERE token_hash IS NOT NULL AND token_hash <> ''
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS claims (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL,
      source TEXT NOT NULL DEFAULT 'venmo',
      pack_id TEXT NOT NULL,
      txn_id TEXT NOT NULL,
      txn_norm TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL DEFAULT 'pending',
      submitted_at BIGINT NOT NULL DEFAULT 0,
      reviewed_at BIGINT NOT NULL DEFAULT 0,
      credited_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS source TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS pack_id TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS txn_id TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS txn_norm TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS status TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS submitted_at BIGINT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS reviewed_at BIGINT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS credited_at BIGINT
  `);
  await db.query(`
    UPDATE claims
    SET source = COALESCE(NULLIF(source, ''), 'venmo'),
        status = COALESCE(NULLIF(status, ''), 'pending'),
        submitted_at = COALESCE(submitted_at, 0),
        reviewed_at = COALESCE(reviewed_at, 0),
        credited_at = COALESCE(credited_at, 0)
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS claims_txn_norm_unique_idx
    ON claims (txn_norm)
    WHERE txn_norm IS NOT NULL AND txn_norm <> ''
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS live_wins (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL,
      vip BOOLEAN NOT NULL DEFAULT FALSE,
      game TEXT NOT NULL,
      amount NUMERIC(14,2) NOT NULL,
      multiplier NUMERIC(10,2),
      submitted_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS vip BOOLEAN
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS game TEXT
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS amount NUMERIC(14,2)
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS multiplier NUMERIC(10,2)
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS submitted_at BIGINT
  `);
  await db.query(`
    UPDATE live_wins
    SET vip = COALESCE(vip, FALSE),
        game = COALESCE(NULLIF(game, ''), 'Casino'),
        amount = COALESCE(amount, 0),
        submitted_at = COALESCE(submitted_at, 0)
  `);
}

async function startServer() {
  try {
    await ensureSchema();
    app.listen(port, () => {
      console.log(`Nights on Wall Street server running at ${baseUrl}`);
    });
  } catch (error) {
    console.error("Failed to start server", error);
    process.exit(1);
  }
}

startServer();
